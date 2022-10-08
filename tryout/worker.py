import os
from threading import Thread
from input import PathInputData
from nltk.tokenize import word_tokenize

class GenericWorker:
    def __init__(self, input_data):
        self.input_data = input_data
        self.result = None

    def map(self):
        raise NotImplementedError

    def reduce(self, other):
        raise NotImplementedError

    @classmethod
    def create_workers(cls, input_class, config):
        workers = []
        for input_data in input_class.generate_inputs(config):
            workers.append(cls(input_data))
        return workers

class InvertedIndexWorker(GenericWorker):
    def map(self):
        self.result = {}
        data = self.input_data.read()
        punc = '''!()-[]{};:'"\, <>./?@#$%^&*_~'''
        line = data.count('\n')
        array = []
        for i in range(line):
            array.append(self.input_data.readline())
        for ele in data:  
            if ele in punc:  
                data = data.replace(ele, " ") 
        data=data.lower()
        for i in range(1):
            text_tokens = word_tokenize(data)
        for i in range(line):
            check = array[i].lower()
            for item in text_tokens:
                if item in check:
                    if item not in self.result:
                        self.result[item] = []
        
                    if item in self.result:
                        self.result[item].append(i+1)
        return self.result

    def reduce(self, other):
        return { key:self.result.get(key,[])+other.result.get(key,[])
                for key in set(list(self.result.keys())+list(other.result.keys())) }
        # return self.result = self.result | other.result

class WordCountWorker(GenericWorker):
    def map(self):
        self.result = {}
        data = self.input_data.read()
        punc = '''!()-[]{};:'"\, <>./?@#$%^&*_~'''
        line = data.count('\n')
        array = []
        for i in range(line):
            array.append(self.input_data.readline())
        for ele in data:  
            if ele in punc:  
                data = data.replace(ele, " ") 
        data=data.lower()
        for i in range(1):
            text_tokens = word_tokenize(data)
        for i in range(line):
            check = array[i].lower()
            for item in text_tokens:
                if item in check:
                    if item not in self.result:
                        self.result[item] = []
                    if item in self.result:
                        self.result[item].append(1)
        return self.result

    def reduce(self, other):
        return { key: sum(self.result[key].values()) + sum(other.result[key].values())
                for key in set(list(self.result.keys())+list(other.result.keys())) }