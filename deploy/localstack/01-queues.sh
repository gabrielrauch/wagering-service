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
#   A message made visible again four times is redriven to the dead-letter
#   queue on the fifth receipt. Five because the failures worth retrying here
#   are transient — a lock conflict, a connection lost, the database restarting
#   — and a handful of attempts spans them; a message that has failed five
#   times is failing for a reason no further delivery will change, and leaving
#   it in the queue would put it at the head of its wallet's group forever.
#
# MessageRetentionPeriod: 1209600 seconds (14 days, the SQS maximum)
#   On both queues. On the dead-letter queue it is the window an operator has
#   to notice and act; on the source queue it is what survives an outage over
#   a long weekend.
#
# ReceiveMessageWaitTimeSeconds: 20 seconds (the SQS maximum)
#   Long polling, as the queue's own default, so that a consumer which does not
#   ask for it still does not spin. The service always asks explicitly, and a
#   request that names its own wait overrides this.
# ---------------------------------------------------------------------------

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

DLQ_URL="$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --output text --query QueueUrl)"
DLQ_ARN="$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names QueueArn --output text --query 'Attributes.QueueArn')"

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

echo "provisioned the wagering queues:"
awslocal sqs list-queues --output text --query 'QueueUrls[]' | tr '\t' '\n' | sed 's/^/  /'
