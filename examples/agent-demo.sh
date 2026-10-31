#!/usr/bin/env bash
# "Agent with undo, memory and forget" — loget in ~40 commands.
# One server, one URL (a local dir here; s3://... works the same).
set -euo pipefail
LOGET=${LOGET:-loget}
DATA=${DATA:-./agent-demo-data}
PORT=${PORT:-7071}
rm -rf "$DATA"
step() { printf '\n== %s\n' "$*"; }
lsn()  { jq -r .lsn; }

$LOGET serve --db "$DATA" --listen 127.0.0.1:$PORT 2>/tmp/loget-demo-server.log &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
export LOGET_ADDR=http://127.0.0.1:$PORT
for i in $(seq 50); do curl -sf $LOGET_ADDR/v1/verify >/dev/null && break; sleep 0.1; done

step "1. Memory: the agent remembers things about alice and bob, each value tagged with its subject"
# Keys are not encrypted, so they use an opaque id (u17); personal data goes in values tagged with a subject.
$LOGET put user/u17/pref "oat milk, no sugar" --subject alice | jq -c .
$LOGET put mem/u17/m1 "alice is allergic to peanuts" --subject alice | jq -c .
$LOGET put mem/u42/m2 "bob prefers window seats" --subject bob | jq -c .
echo "-- recall: memories about u17"; $LOGET scan mem/u17/ | jq -c '.[]'

step "2. Tasks: the result and the task's status commit atomically in ONE transaction"
$LOGET put task/t1 '{"do":"order lunch for alice","state":"open"}' --subject alice | jq -c .
$LOGET put task/t2 '{"do":"book bob a flight","state":"open"}' | jq -c .
$LOGET tx '[{"op":"put","key":"result/t1","value":"ordered salad (no peanuts)","subject":"alice"},
           {"op":"put","key":"task/t1","value":"{\"do\":\"order lunch for alice\",\"state\":\"done\"}","subject":"alice"}]' | jq -c .
echo "-- tasks:"; $LOGET scan task/ | jq -c '.[]'
GOOD=$($LOGET put checkpoint "before-mistake" | lsn)
echo "-- checkpoint at LSN $GOOD"

step "3. Undo: the agent goes rogue on main..."
$LOGET put user/u17/pref "ESPRESSO x4" --subject alice | jq -c .
$LOGET del result/t1 | jq -c .
echo "-- main now says: $($LOGET get user/u17/pref)"; $LOGET get result/t1 || true
echo "-- time travel: at LSN $GOOD alice's pref was: $($LOGET get user/u17/pref --at $GOOD)"
echo "-- rewind main to LSN $GOOD (old head is kept in the reflog, nothing is deleted)"
$LOGET rewind main $GOOD | jq -c .
echo "-- main says: $($LOGET get user/u17/pref) / result/t1 = $($LOGET get result/t1)"

step "4. Try before you buy: a risky plan on a branch, then promote it"
$LOGET branch create plan-b --from main | jq -c .
$LOGET put result/t2 "booked bob seat 12A" --branch plan-b | jq -c .
echo "-- main sees result/t2? "; $LOGET get result/t2 || true
echo "-- plan-b sees: $($LOGET get result/t2 --branch plan-b)"
$LOGET branch promote plan-b main | jq -c .
echo "-- after promote, main sees: $($LOGET get result/t2)"

step "5. Forget: alice invokes her right to erasure — crypto-shred subject 'alice'"
$LOGET shred alice | jq -c '{valid_signature, shred_lsn: .certificate.shred_lsn, dek_deleted: .certificate.dek_deleted, chain_head: (.certificate.chain_head[0:16]+"..."), signature: (.certificate.signature[0:16]+"...")}'
echo "-- KV:";            $LOGET get user/u17/pref || true
echo "-- time travel:";   $LOGET get user/u17/pref --at $GOOD || true
echo "-- old branch:";    $LOGET get result/t1 --branch plan-b || true
echo "-- memories:";      $LOGET scan mem/ | jq -c '.[]'
echo "-- task t1:";        $LOGET get task/t1 || true
echo "-- bob is untouched: $($LOGET get mem/u42/m2)"
echo "-- new writes for alice are refused:"; $LOGET put user/u17/x y --subject alice || true

step "6. Verify the whole history (CRCs, hash chains, fencing, shred proofs)"
$LOGET verify | jq -c '{ok, logs, segments, entries, encrypted_values, unreadable_shredded_values, problems}'
kill $SERVER; wait $SERVER 2>/dev/null || true
echo "-- grep every byte in the bucket for anything about alice:"
if grep -rqai -e "alice" -e "peanuts" -e "oat milk" -e "ESPRESSO" "$DATA"; then echo "FOUND PLAINTEXT (bad)"; grep -rlai -e alice -e peanuts "$DATA"; exit 1; else echo "no trace of 'alice', 'peanuts', 'oat milk' or 'ESPRESSO' in $DATA"; fi
