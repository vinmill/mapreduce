from distutils.command.config import config
import os
import ast
from threading import Thread
import nltk
from nltk.tokenize import word_tokenize
import random
nltk.download('punkt')

class InputData:
    def read(self):
        raise NotImplementedError
    @classmethod
    def generate_inputs(cls, config):
        raise NotImplementedError

class TextInputData(InputData):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def read(self):
        with open(self.path) as f:
            return f.read()

    @classmethod
    def generate_inputs(cls, config):
        data_dir = config["raw_dir"]
        for name in os.listdir(data_dir):
            yield cls(os.path.join(data_dir, name))

class JsonInputData(InputData):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def read(self):
        with open(self.path) as f:
            dat = f.read()
            return ast.literal_eval(dat)

    @classmethod
    def generate_inputs(cls, config):
        data_dir = config["intermediate_dir"]
        for name in os.listdir(data_dir):
            yield cls(os.path.join(data_dir, name))

class Worker:
    def __init__(self, inputs):
        self.inputs = inputs
        self.outputs = None

    def mapper(self):
        raise NotImplementedError

    def reducer(self):
        raise NotImplementedError

    @classmethod
    def create_workers(cls, input_class, config):
        workers = []
        for inputs in input_class.generate_inputs(config):
            workers.append(cls(inputs))
        return workers

class InvertedIndexWorker(Worker):
    def mapper(self):
        self.outputs = {}
        data = self.inputs.read()
        array = data.splitlines()
        punc = '''!()-[]{};:'"\, <>./?@#$%^&*_~'''
        for ele in data:  
            if ele in punc:  
                data = data.replace(ele, " ") 
        data=data.lower()
        for i in range(len(array)):
            check = array[i].lower()
            text_tokens = word_tokenize(check)
            for item in text_tokens:
                if item in check:
                    if item not in self.outputs:
                        self.outputs[item] = []
        
                    if item in self.outputs:
                        self.outputs[item].append(i+1)
        return self.outputs

    def reducer(self):
        output = self.inputs.read()
        self.outputs = { key: output.get(key,[]) for key in set(list(output.keys())) }
        return self.outputs


class WordCountWorker(Worker):
    def mapper(self):
        self.outputs = {}
        data = self.inputs.read()
        array = data.splitlines()
        punc = '''!()-[]{};:'"\, <>./?@#$%^&*_~'''
        for ele in data:  
            if ele in punc:  
                data = data.replace(ele, " ") 
        data=data.lower()
        for i in range(len(array)):
            check = array[i].lower()
            text_tokens = word_tokenize(check)
            for item in text_tokens:
                if item in check:
                    if item not in self.outputs:
                        self.outputs[item] = []
                    if item in self.outputs:
                        self.outputs[item].append(1)
        return self.outputs

    def reducer(self):
        output = self.inputs.read()
        self.outputs = { key: sum(value) for key, value in output.items() }
        return self.outputs


class LineCountWorker(Worker):
    def mapper(self):
        data = self.inputs.read()
        self.outputs = data.count("\n")
    def reducer(self, other):
        self.outputs += other.outputs

def executemapper(workers):
    threads = [Thread(target=w.mapper) for w in workers]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    first, *rest = workers
    return first.outputs

def executereducer(workers):
    threads = [Thread(target=w.reducer) for w in workers]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    first, *rest = workers
    return first.outputs


def mapperfunction(map_worker_class, input_class, config):
    mapworkers = map_worker_class.create_workers(input_class, config)
    return executemapper(mapworkers)

def reducerfunction(reduce_worker_class, input_class, config):
    reduceworkers = reduce_worker_class.create_workers(input_class, config)
    return executereducer(reduceworkers)

config = {"data_dir": '/Users/main/Documents/repos/Cloud-Computing-Assignment2/raw-data/',
        "intermediate_dir": '/Users/main/Documents/repos/Cloud-Computing-Assignment2/intermediate_data/'}
intermediate = mapperfunction(WordCountWorker, TextInputData, config)
outputs = reducerfunction(WordCountWorker, JsonInputData, config)

print(outputs)