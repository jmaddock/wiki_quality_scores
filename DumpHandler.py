import subprocess
import utils
import os

## Class for decompressing, iterating through, and re-compressing wiki xml dumps
class DumpHandler(object):
    def __init__(self,f_in):
        self.f_in = f_in
        self.uncompressed = f_in.rsplit('.7z', 1)[0]
        self.dump = None
        self.base_dir = f_in.rsplit('/', 1)[0]

    # decompress a dump from a .7z archive
    # return decompressed file path
    def decompress(self):
        utils.log('decompressing file: %s' % self.f_in)
        subprocess.call(['7z','x',self.f_in,'-o' + self.base_dir])
        return self.uncompressed

    # remove the decompressed dump after processing
    def remove_dump(self):
        self.dump = None
        utils.log('removing file: %s' % self.uncompressed)
        subprocess.call(['rm',self.uncompressed])

    # convenience method for decompressing and opening a dump
    def process_dump(self):
        if os.path.exists(self.uncompressed):
            self.remove_dump()
        self.decompress()
        self.open_dump()
        return self.dump