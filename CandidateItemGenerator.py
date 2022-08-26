#! /usr/bin/python3
# CandidateItemGenerator.py
from mrjob.job import MRJob
from itertools import combinations as cb
from itertools import chain as join_subarrays
import json



class CandidateItemGenerator(MRJob):

    def configure_args(self):
        '''This function mentioned external broadcast variables for the MR job'''
        super(CandidateItemGenerator, self).configure_args()
        self.add_passthru_arg('--koperation', type=int, default=1)
        self.add_file_arg('--varfile')

    def load_args(self, args):
        '''In this function we load all the broadcast variable in the memory of our workers'''
        super(CandidateItemGenerator, self).load_args(args)
        self.iterx=self.options.koperation
        self.varx=self.options.varfile
        with open(self.varx,"r") as f:
            json_contents=f.read()
            if json_contents:
                self.last_round_json=list(join_subarrays(*[
                    json.loads(key)
                    for key in json.loads(json_contents).keys()
                ]))

    def mapper(self, _, line):
        '''
        In this function we parse the input file and map their values we also
        We also remove invalid variables in the first step.
        We also remove items from the previous step
        we also in increment counter in the first pass of the algorithm
        We also need to sort the items yielded from the mapper so that they match and merge after reducing
        '''
        lineitems = [amenity[2:-2].strip() for amenity in line[2:-2].split(', ')]
        if not self.iterx==1:
            lineitems=list(set(self.last_round_json).intersection(set(lineitems)))
        else:
            self.increment_counter("association_rules", 'transaction_count', 1)
            lineitems= [item for item in lineitems if item]
        lineitems.sort()
        for item in cb(lineitems,self.iterx):
                yield json.dumps(item),1

    def combiner(self, item, counts):
        yield item, sum(counts)

    def reducer(self, item, counts):
        num_count=sum(counts)
        yield item, num_count

if __name__ == '__main__':
    CandidateItemGenerator.run()


