from array import array
from distutils.command.config import config
import os
import ast
from threading import Thread
import nltk
import yaml
from nltk.tokenize import word_tokenize
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
        data_dir = config['WORKERS']['RAWDATA']
        for name in os.listdir(data_dir):
            yield cls(os.path.join(data_dir, name))

class DictionaryInputData(InputData):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def read(self):
        with open(self.path) as f:
            dat = f.read()
            return ast.literal_eval(dat)

    @classmethod
    def generate_inputs(cls, config):
        data_dir = config['WORKERS']['INTERIMDATA']
        for name in os.listdir(data_dir):
            yield cls(os.path.join(data_dir, name))

# class Worker:
#     def __init__(self, inputs):
#         self.inputs = inputs
#         self.outputs = None

#     def mapper(self):
#         raise NotImplementedError

#     def reducer(self):
#         raise NotImplementedError

#     @classmethod
#     def create_workers(cls, input_class, config):
#         workers = []
#         for inputs in input_class.generate_inputs(config):
#             workers.append(cls(inputs))
#         return workers

class InvertedIndexWorker:
    def __init__(self):
        self.inputs = None
        self.outputs = {}
        
    def mapper(self, data):
        arr = data
        punc = '''!()-[]{};:'"\, <>./?@#$%^&*_~'''
        for ele in range(len(arr)):
            arr[ele] = arr[ele].strip("""\n""")
            arr[ele] = arr[ele].strip("""\ufeff""")
            for i in ele:
                if i in punc:  
                    ele = ele.replace(i, " ") 
        data=data.lower()
        for i in range(len(arr)):
            check = arr[i].lower()
            text_tokens = word_tokenize(check)
            for item in text_tokens:
                if item in check:
                    if item not in self.outputs:
                        self.outputs[item] = []
        
                    if item in self.outputs:
                        self.outputs[item].append(i+1)
        return self.outputs

    def reducer(self, data):
        output = data
        self.outputs = { key: output.get(key,[]) for key in set(list(output.keys())) }
        return self.outputs


class WordCountWorker(object):
    def __init__(self):
        self.inputs = None
        self.outputs = {}

    def mapper(self, data):
        arr = data
        if isinstance(arr, str):
            arr = arr.strip('][').split(', ')
        punc = '''!()-[]{};:'"\, <>./?@#$%^&*_~'''
        for ele in range(len(arr)):
            arr[ele] = arr[ele].strip("""\n""")
            arr[ele] = arr[ele].strip("""\ufeff""")
            for i in ele:
                if i in punc:  
                    ele = ele.replace(i, " ") 
        for i in range(len(arr)):
            check = arr[i].lower()
            text_tokens = word_tokenize(check)
            for item in text_tokens:
                if item in check:
                    if item not in self.outputs:
                        self.outputs[item] = []
                    if item in self.outputs:
                        self.outputs[item].append(1)
        return self.outputs

    def reducer(self, data):
        output = data
        self.outputs = { key: sum(value) for key, value in output.items() }
        return self.outputs


class LineCountWorker(object):
    def __init__(self, data):
        self.outputs = {}
        self.data = data
        
    def mapper(self, data):
        self.outputs = data.count("\n")
        
    def reducer(self, other):
        self.outputs += other.outputs


def executemapper(worker, data):
    x = worker()
    x = x.mapper(data)
    return x

def executereducer(worker, data):
    x = worker()
    x = x.reducer(data)
    return x

def mapperfunction(map_worker_class, data):
    return executemapper(map_worker_class, data)

def reducerfunction(reduce_worker_class, data):
    return executereducer(reduce_worker_class, data)

