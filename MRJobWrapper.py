import os
import sys
import json
import time
from itertools import combinations as cb
from CandidateItemGenerator import CandidateItemGenerator

# Parse arguments
support_threshold = float(sys.argv[1])
k = int(sys.argv[2])
args = sys.argv[3:6]  # mode, runner, confidence
confidence_threshold = float(sys.argv[6])

if k == 0:
    sys.exit()

# Prepare MRJob args
args.extend(["--koperation", "NUMBER", "--varfile", "varx.json"])
open("varx.json", "w").close()

transaction_count = 0
end_round = 0
frequent_itemsets = []

# Run k passes
for i in range(1, k + 1):
    start = time.time()
    args[4] = str(i)  # set koperation

    # If not first pass, use previous frequent set file
    if i > 1:
        args[args.index("--varfile") + 1] = f"xfrequent{i - 1}.txt"

    job = CandidateItemGenerator(args=args)
    with job.make_runner() as runner:
        runner.run()

        if i == 1:
            counters = runner.counters()
            transaction_count = counters[0]["association_rules"]["transaction_count"]

        # Filter frequent itemsets
        filex = {
            key: value
            for key, value in job.parse_output(runner.cat_output())
            if value / transaction_count >= support_threshold
        }

    print(f"Time taken for pass {i}: {round(time.time() - start, 2)}s")

    if not filex:
        break

    with open(f"xfrequent{i}.txt", "w") as fh:
        json.dump(filex, fh)

    frequent_itemsets.append(filex)
    end_round = i

# Cleanup and load all frequent sets
for i in range(1, end_round + 1):
    with open(f"xfrequent{i}.txt") as f:
        frequent_itemsets[i - 1] = json.load(f)
    os.remove(f"xfrequent{i}.txt")

print(f"No of rounds processed: {end_round} | Rounds Skipped: {k - end_round}")

# Generate association rules
rule_count = 0
for j in reversed(range(end_round)):
    for x in range(j):
        for key, val in frequent_itemsets[j].items():
            arr_curr = json.loads(key)
            for e in cb(arr_curr, x + 1):
                antecedent = json.dumps(list(e))
                if antecedent in frequent_itemsets[x]:
                    conf = val / frequent_itemsets[x][antecedent]
                    if conf >= confidence_threshold:
                        consequent = sorted(set(arr_curr) - set(e))
                        print(
                            set(e),
                            "->",
                            set(consequent),
                            f"Confidence: {round(conf, 4)}",
                            end=" ",
                        )
                        if json.dumps(consequent) in frequent_itemsets[j - x - 1]:
                            valy = frequent_itemsets[j - x - 1][json.dumps(consequent)]
                            print(
                                f"Interest: {round(conf - valy / transaction_count, 6)}"
                            )
                        rule_count += 1

print(f"Number of Association rules produced: {rule_count}")
