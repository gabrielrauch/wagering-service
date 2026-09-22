#!/bin/bash
# Provisions the queues the wagering service reads from and writes to.
#
# LocalStack runs every executable in /etc/localstack/init/ready.d once the edge
# service is accepting requests, so this needs no readiness loop of its own. It
# also means the queues appear a moment AFTER the container has logged "Ready.",
# so anything waiting on them must wait for a queue and not for the container.
#
# Every call goes through `awslocal`, the wrapper LocalStack ships on PATH. It
# supplies the endpoint and the dummy credentials the AWS CLI insists on; a
# plain `aws` here fails with "Unable to locate credentials" and the init stage
# reports a non-zero exit long after the queues were supposed to exist. Do not
# wrap it in a helper of the same name either — the helper shadows the wrapper
# and the failure looks identical.
set -euo pipefail

# ---------------------------------------------------------------------------
# What the parameters below mean, in one place, because two of them are the
# whole of this system's ordering and deduplication contract.
#
# MessageGroupId
#   Inbound (wager-transactions.fifo): the WALLET ID. FIFO orders within a
#   group and only within a group, and a wallet is the unit whose operations
#   must not overtake each other — a rollback that arrived after the bet it
#   undoes must be handled after it. Grouping by anything wider (one group for
#   the queue) would serialise every wallet behind every other; grouping by
#   anything narrower (the transaction) would order nothing at all.
#   Outbound (wallet-events.fifo): the AGGREGATE ID, which for every event this
#   service publishes is also a wallet — a consumer rebuilding a wallet's
#   history reads one group in the order the aggregate sequence was assigned.
#
# MessageDeduplicationId
#   Inbound: the MESSAGE ID from the envelope. It is the identity the inbox
#   keys on, so the queue's five-minute deduplication window and the database's
#   permanent record agree on what "the same message" means. ContentBasedDedup
#   is off for exactly that reason: two bodies differing only in whitespace are
#   one message here, and a content hash would make them two.
#   Outbound: the EVENT ID. It is stable across republication, so a publisher
#   killed between sending and marking the outbox row cannot put a second copy
#   of an event on the wire when it comes back and sends again.
#
# VisibilityTimeout: 30 seconds
#   How long a received message is hidden from other consumers. It has to
#   outlast one consumer's whole unit of work — read the inbox, take the wallet
#   lock, write the transaction, the ledger entry and the outbox row, commit —
#   and it has to be short enough that work abandoned by a consumer that died
#   comes back quickly. The consumer does not rely on it alone: it extends the
#   timeout with a backoff when it means to try again later, and sets it to 0
#   at shutdown so an in-flight message is redelivered at once rather than
#   waiting the timeout out.
#
# Retry limits: maxReceiveCount 5
#   SQS moves a message to the dead-letter queue when its receive count
#   EXCEEDS this number — the fifth delivery is still handled, and it is when
#   that one is spent and a sixth is due that the message is moved. Five
#   because the failures worth retrying here are transient — a lock conflict,
#   a connection lost, the database restarting — and a handful of attempts
#   spans them; a message that has failed five times is failing for a reason
#   no further delivery will change, and leaving it in the queue would put it
#   at the head of its wallet's group forever.
#
# MessageRetentionPeriod: 1209600 seconds (14 days, the SQS maximum)
#   On all three queues. On the dead-letter queue it is the window an operator
#   has to notice and act; on the two live queues it is what survives an outage
#   over a long weekend.
#
# ReceiveMessageWaitTimeSeconds: 20 seconds (the SQS maximum)
#   Long polling, as the queue's own default, so that a consumer which does not
#   ask for it still does not spin. On the two live queues and not on the
#   dead-letter queue, which nothing polls in a loop. The service always asks
#   explicitly, and a request that names its own wait overrides this.
#
# Policy: who may do what, by IAM role
#   Access to the broker is controlled by the broker: a caller's credentials
#   identify a principal, and the resource policy on each queue says what that
#   principal may call on it. Three roles in the account, and the least each
#   can work with:
#
#     wagering-producer   the game providers' integration. sqs:SendMessage on
#                         the inbound queue and nothing else — it puts
#                         operations on the wire and never reads them back.
#     wagering-worker     the consumer and the outbox publisher. On the inbound
#                         queue: receive, delete and change the visibility of
#                         messages, which is the consumer's whole vocabulary.
#                         On the outbound queue: send, which is the
#                         publisher's. The only role that may receive from the
#                         dead-letter queue. On all three, GetQueueUrl and
#                         GetQueueAttributes, because it resolves its queues
#                         at start-up.
#     wagering-api        the HTTP replicas. GetQueueUrl and GetQueueAttributes
#                         on the two live queues, for the start-up check and
#                         the readiness probe, and SendMessage nowhere: the
#                         API never puts anything on a queue itself. A
#                         submission becomes an outbox row, and the worker
#                         publishes it.
#
#   No statement grants to "*", and there is no Deny: a principal the policy
#   does not name is refused by default.
#
#   LocalStack Community does not enforce IAM. The policy is provisioned and
#   can be read back — the integration suite reads it and checks every grant
#   above — but a call the policy would deny still succeeds here, so a refusal
#   cannot be demonstrated locally. On AWS it is enforced, and the three roles
#   must exist in the account before this runs, because SQS refuses a policy
#   naming a principal it cannot resolve. docker-compose.yml sets each
#   service's AWS_ACCESS_KEY_ID to the name of the role it is meant to run as,
#   so the intended identity is at least visible where the credentials are.
# ---------------------------------------------------------------------------

# The account is LocalStack's fixed one. On AWS the same three ARNs carry the
# real account id, and the roles have to exist before the queues do.
ACCOUNT="000000000000"
PRODUCER_ROLE="arn:aws:iam::${ACCOUNT}:role/wagering-producer"
WORKER_ROLE="arn:aws:iam::${ACCOUNT}:role/wagering-worker"
API_ROLE="arn:aws:iam::${ACCOUNT}:role/wagering-api"

# queue_url and queue_arn look a queue up by name once it exists. The ARN is
# what a redrive policy and a resource policy both name a queue by, and neither
# can be formed for a queue that does not exist yet — which is why every queue
# is created first and configured after.
queue_url() {
  awslocal sqs get-queue-url --queue-name "$1" --output text --query QueueUrl
}
queue_arn() {
  awslocal sqs get-queue-attributes --queue-url "$1" \
    --attribute-names QueueArn --output text --query 'Attributes.QueueArn'
}

# set_policy attaches a resource policy to a queue.
#
# The policy is a JSON document carried as a STRING inside the attributes JSON,
# so every quote in it is escaped once and the newlines it was written with are
# dropped — the same shape the redrive policy below is written in by hand, done
# here by the shell because a policy is ten times as long.
set_policy() {
  local policy
  policy="$(tr -d '\n' <<<"$2")"
  awslocal sqs set-queue-attributes --queue-url "$1" \
    --attributes "{\"Policy\":\"${policy//\"/\\\"}\"}"
}

# The dead-letter queue is created first because the source queue's redrive
# policy names it by ARN, and an ARN cannot be formed for a queue that does not
# exist yet.
awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes '{
    "FifoQueue":"true",
    "ContentBasedDeduplication":"false",
    "MessageRetentionPeriod":"1209600"
  }' >/dev/null

DLQ_URL="$(queue_url wager-transactions-dlq.fifo)"
DLQ_ARN="$(queue_arn "$DLQ_URL")"

# Receivable by the worker role and by nothing else. Nothing sends to it
# directly: SQS moves messages here itself under the redrive policy, which
# needs no grant from this queue.
set_policy "$DLQ_URL" "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WorkerInspectsTheDeadLetterQueue",
      "Effect": "Allow",
      "Principal": {"AWS": "${WORKER_ROLE}"},
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "${DLQ_ARN}"
    }
  ]
}
EOF
)"

# The queue the consumer reads. MessageGroupId is the wallet id and
# MessageDeduplicationId is the envelope's message id — see the note above.
awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "{
    \"FifoQueue\":\"true\",
    \"ContentBasedDeduplication\":\"false\",
    \"VisibilityTimeout\":\"30\",
    \"MessageRetentionPeriod\":\"1209600\",
    \"ReceiveMessageWaitTimeSeconds\":\"20\",
    \"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":5}\"
  }" >/dev/null

INBOUND_URL="$(queue_url wager-transactions.fifo)"
INBOUND_ARN="$(queue_arn "$INBOUND_URL")"

# The producer sends, the worker consumes, the API only looks.
set_policy "$INBOUND_URL" "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ProducerSendsOperations",
      "Effect": "Allow",
      "Principal": {"AWS": "${PRODUCER_ROLE}"},
      "Action": "sqs:SendMessage",
      "Resource": "${INBOUND_ARN}"
    },
    {
      "Sid": "WorkerConsumesOperations",
      "Effect": "Allow",
      "Principal": {"AWS": "${WORKER_ROLE}"},
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "${INBOUND_ARN}"
    },
    {
      "Sid": "APIChecksReadiness",
      "Effect": "Allow",
      "Principal": {"AWS": "${API_ROLE}"},
      "Action": [
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "${INBOUND_ARN}"
    }
  ]
}
EOF
)"

# The destination the outbox publisher sends to. MessageGroupId is the
# aggregate id and MessageDeduplicationId is the event id — see the note above.
# No redrive policy: nothing in this deployment consumes it, and a dead-letter
# queue for a destination nobody reads would collect nothing.
awslocal sqs create-queue \
  --queue-name wallet-events.fifo \
  --attributes '{
    "FifoQueue":"true",
    "ContentBasedDeduplication":"false",
    "MessageRetentionPeriod":"1209600",
    "ReceiveMessageWaitTimeSeconds":"20"
  }' >/dev/null

OUTBOUND_URL="$(queue_url wallet-events.fifo)"
OUTBOUND_ARN="$(queue_arn "$OUTBOUND_URL")"

# The worker sends — it is where the outbox publisher writes — and the API
# only looks. No consumer is named because this deployment has none; the
# one that arrives gets a statement of its own here.
set_policy "$OUTBOUND_URL" "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WorkerPublishesEvents",
      "Effect": "Allow",
      "Principal": {"AWS": "${WORKER_ROLE}"},
      "Action": [
        "sqs:SendMessage",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "${OUTBOUND_ARN}"
    },
    {
      "Sid": "APIChecksReadiness",
      "Effect": "Allow",
      "Principal": {"AWS": "${API_ROLE}"},
      "Action": [
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "${OUTBOUND_ARN}"
    }
  ]
}
EOF
)"

echo "provisioned the wagering queues:"
awslocal sqs list-queues --output text --query 'QueueUrls[]' | tr '\t' '\n' | sed 's/^/  /'
