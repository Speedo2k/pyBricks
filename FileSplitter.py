import sys
import argparse

cli_parser = None

def arg_parse():
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("-j", "--join", help="join files (default: split)", action="store_true")
    cli_parser.add_argument('file_name', help='file_name for split or join')
    cli_parser.add_argument("-s", "--size", type=int, help='file size when split')
    cli_parser.add_argument("-l", "--lines", type=int, help='lines iin file when split')
    cli_parser.add_argument("-wd", help='working directory for split and join(not implemented yet)')
    return cli_parser.parse_args()



class FileSplitter:
    def __init__(self, input_name, output_name, max_size=None, max_lines=None):
        self.input_name = input_name
        self.output_name = output_name
        self.max_size = max_size or sys.maxsize
        self.chunk_size = min(8192, self.max_size)
        self.max_lines = max_lines or sys.maxsize
        self.is_text_file = self.max_lines < sys.maxsize

    def txt_reader(self, receiver, chunk_size=8192):
        r = receiver()
        r.send(None)
        with open(self.input_name, 'r') as input:
            for line in input:
                r.send(line)

    def bin_reader(self, receiver, chunk_size=8192):
        r = receiver()
        r.send(None)
        with open(self.input_name, 'rb') as input:
            while chunk := (input.read(chunk_size)):
                r.send(chunk)

    def out_fn_seq(self, name, seq):
        return name+'.'+str(seq).zfill(3)

    def txt_writer(self):
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

    def bin_writer(self):
        fseq = 0
        try:
            while True:
                with  open(self.out_fn_seq(self.output_name, fseq), 'wb') as output:
                    size = 0
                    fseq +=1
                    while size < self.max_size:
                        chunk = (yield)
                        output.write(chunk)
                        size += len(chunk)
        except GeneratorExit:
            pass


    def split(self):
        if self.is_text_file:
            self.txt_reader(self.txt_writer)
        else:
            self.bin_reader(self.bin_writer, chunk_size=self.chunk_size)

    def join(self, chunk_size=8192):
        with open(self.input_name+'.all', 'wb') as output:
            fseq = 0
            try:
                while True:
                    with open(self.out_fn_seq(self.output_name, fseq), 'rb') as input:
                        while chunk := input.read(chunk_size):
                            output.write(chunk)
                    fseq +=1
            except FileNotFoundError:
                pass


if __name__ == '__main__':
    args = arg_parse()
    fn =  args.file_name
    fs = FileSplitter(fn, fn, max_lines=args.lines, max_size=args.size)
    print (f'Fn {fn}, action:{args.join}, lines={args.lines}, size={args.size}')
    if args.join:
        fs.join()
    else:
        fs.split()
