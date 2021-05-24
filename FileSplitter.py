import sys

import sys

class FileSplitter:
    def __init__(self, input_name, output_name, max_size=None, max_lines=None):
        self.input_name = input_name
        self.output_name = output_name
        self.max_size = max_size
        if self.max_size is None or self.max_size == 0:
           self. max_size = sys.maxsize
        self.max_lines = max_lines
        if self.max_lines is None or self.max_lines == 0:
            self.max_lines = sys.maxsize

    def reader(self, receiver):
        r = receiver()
        r.send(None)
        with open(self.input_name, 'r') as input:
            for line in input:
                r.send(line)

    def out_fn_seq(self, name, seq):
        return name+'.'+str(seq).zfill(3)

    def writer(self):
        fseq = 0
        try:
            while True:
                with  open(self.out_fn_seq(self.output_name, fseq), 'w') as output:
                    size = 0
                    lines = 0
                    fseq +=1
                    while (size < self.max_size) and (lines < self.max_lines):
                        line = (yield)
                        output.write(line)
                        size += len(line)+2
                        lines +=1
        except GeneratorExit:
            pass

    def split(self):
        self.reader(self.writer)

    def join(self):
        with open(self.input_name+'.all', 'w') as output:
            fseq = 0
            try:
                while True:
                    with open(self.out_fn_seq(self.output_name, fseq), 'r') as input:
                        for line in input:
                            output.write(line)
                    fseq +=1
            except FileNotFoundError:
                pass

if __name__ == '__main__':


    fs = FileSplitter('textfilesplitter.py', 'test.pyf', max_lines=10)
    fs.split()
    fs.join()
