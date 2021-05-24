import sys
import os.path
import argparse
import base64


def arg_parse():

    def exit_with_error(msg):
        cli_parser.print_help()
        print(msg)
        exit(1)

    cli_parser = argparse.ArgumentParser(description='Split files into smaller ones based on size/lines or join them.')
    cli_parser.add_argument("-j", "--join", help="join files (default: split)", action="store_true")
    cli_parser.add_argument("--base64", help="apply bas64 encode/decode", action="store_true")
    cli_parser.add_argument('file_name', help='file_name for split or join')
    cli_parser.add_argument("-s", "--size", type=int, help='file size when split')
    cli_parser.add_argument("-l", "--lines", type=int, help='lines iin file when split')
    cli_parser.add_argument("--wd", help='working directory for split and join(not implemented yet)')
    params = cli_parser.parse_args()
    if (fn := params.file_name) and not os.path.isfile(fn):
        exit_with_error(f'ERROR: File name {fn} does not exist!')
    if (wd := params.wd) and not os.path.isdir(wd):
        exit_with_error(f'ERROR: Folder name {wd} does not exist!')
    return params


class FileSplitter:
    def __init__(self, input_name, output_name, max_size=None, max_lines=None, base64=None):
        self.input_name = input_name
        self.output_name = output_name
        self.max_size = max_size or sys.maxsize
        self.chunk_size = min(8192, self.max_size)
        self.max_lines = max_lines or sys.maxsize
        self.is_text_file = self.max_lines < sys.maxsize
        self.base64 = base64
        self.seq = 0

    def base64_encode(self):
        with open(self.input_name, 'rb') as f_in:
            with open(self.output_name+'.b64i', 'wb') as f_out:
                base64.encode(f_in, f_out)
        self.input_name = self.output_name+'.b64i'

    def base64_decode(self):
        with open(self.output_name+'.b64o', 'rb') as f_in:
            with open(self.output_name+'.all', 'wb') as f_out:
                base64.decode(f_in, f_out)

    def txt_reader(self, receiver, chunk_size=8192):
        r = receiver()
        r.send(None)
        with open(self.input_name, 'r') as f_in:
            for line in f_in:
                r.send(line)

    def bin_reader(self, receiver, chunk_size=8192):
        r = receiver()
        r.send(None)
        with open(self.input_name, 'rb') as f_in:
            while chunk := (f_in.read(chunk_size)):
                r.send(chunk)

    @staticmethod
    def next_fn(name, seq):
        return name+'.'+str(seq).zfill(3)

    def txt_writer(self):
        self.seq = 0
        try:
            while True:
                with open(self.next_fn(self.output_name, self.seq), 'w') as f_out:
                    size = 0
                    lines = 0
                    self.seq += 1
                    while (size < self.max_size) and (lines < self.max_lines):
                        line = (yield)
                        f_out.write(line)
                        size += len(line)+2
                        lines += 1
        except GeneratorExit:
            pass

    def bin_writer(self):
        self.seq = 0
        try:
            while True:
                with open(self.next_fn(self.output_name, self.seq), 'wb') as f_out:
                    size = 0
                    self.seq += 1
                    while size < self.max_size:
                        chunk = (yield)
                        f_out.write(chunk)
                        size += len(chunk)
        except GeneratorExit:
            pass

    def split(self):
        if self.base64:
            self.base64_encode()
        if self.is_text_file:
            self.txt_reader(self.txt_writer)
        else:
            self.bin_reader(self.bin_writer, chunk_size=self.chunk_size)

    def join(self, chunk_size=8192):
        if self.base64:
            ext = '.b64o'
        else:
            ext = '.all'
        with open(self.output_name+ext, 'wb') as f_out:
            self.seq = 0
            try:
                while True:
                    with open(self.next_fn(self.output_name, self.seq), 'rb') as f_in:
                        while chunk := f_in.read(chunk_size):
                            f_out.write(chunk)
                    self.seq += 1
            except FileNotFoundError:
                pass
        if self.base64:
            self.base64_decode()


def file_in_working_dir(path, wd):
    fn = os.path.basename(path)
    return os.path.join(wd, fn)


if __name__ == '__main__':
    args = arg_parse()
    fni = args.file_name
    if working_dir := args.wd:
        fno = file_in_working_dir(fni, working_dir)
    else:
        fno = fni
    fs = FileSplitter(fni, fno, max_lines=args.lines, max_size=args.size, base64=args.base64)
    print (f'FileSplitter fni: {fni}, fno: {fno}, wd: {args.wd}, join:{args.join}, base64: {args.base64}, '
           f'lines={args.lines}, size={args.size}')
    if args.join:
        fs.join()
        print(f' - files join into {fno}.all')
    else:
        fs.split()
        print(f' - file {fni} splits into {fs.seq} file(s) from: {fs.next_fn(fno, 0)} to {fs.next_fn(fno, fs.seq - 1)}')
