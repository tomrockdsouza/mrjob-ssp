#!/usr/bin/python3
from mrjob.job import MRJob
from itertools import combinations as cb, chain
import json


class CandidateItemGenerator(MRJob):
    def configure_args(self):
        """Add external broadcast variables for the MR job."""
        super().configure_args()
        self.add_passthru_arg("--koperation", type=int, default=1)
        self.add_file_arg("--varfile")

    def load_args(self, args):
        """Load broadcast variables into worker memory."""
        super().load_args(args)
        self.iterx = self.options.koperation
        self.varx = self.options.varfile

        with open(self.varx, "r") as f:
            contents = f.read()
            if contents:
                # Flatten all keys from the JSON into one list
                self.last_round_json = list(
                    chain(*[json.loads(key) for key in json.loads(contents).keys()])
                )

    def mapper(self, _, line):
        """
        Parse each line into items.
        On first pass: count transactions and remove empty items.
        On later passes: filter items using results from previous round.
        """
        items = [amenity[2:-2].strip() for amenity in line[2:-2].split(", ")]

        if self.iterx == 1:
            self.increment_counter("association_rules", "transaction_count", 1)
            items = [item for item in items if item]
        else:
            items = list(set(self.last_round_json) & set(items))

        items.sort()
        for combo in cb(items, self.iterx):
            yield json.dumps(combo), 1

    def combiner(self, item, counts):
        yield item, sum(counts)

    def reducer(self, item, counts):
        yield item, sum(counts)


if __name__ == "__main__":
    CandidateItemGenerator.run()
