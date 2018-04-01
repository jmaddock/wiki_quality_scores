import subprocess
import os

## Class for decompressing, iterating through, and re-compressing wiki xml dumps
class DumpHandler(object):
    def __init__(self,f_in,**kwargs):
        self.f_in = f_in
        self.uncompressed = f_in.rsplit('.7z', 1)[0]
        self.dump = None
        self.base_dir = f_in.rsplit('/', 1)[0]
        if 'overwrite' in kwargs:
            self.overwrite = True
        else:
            self.overwrite = False
        # some logging stuff
        if 'logger' in kwargs:
            self.logger = kwargs['logger']
        else:
            self.logger = None

    # decompress a dump from a .7z archive
    # return decompressed file path
    def decompress(self):
        if self.logger:
            self.logger.info('decompressing file: %s' % self.f_in)
        if self.overwrite:
            subprocess.call(['7z', 'x', self.f_in, '-o' + self.base_dir, '-bso0', '-bsp0', '-aoa'])
        elif os.path.exists(self.uncompressed):
            raise FileExistsError('decompressed file {0} already exists'.format(self.uncompressed))
        else:
            subprocess.call(['7z', 'x', self.f_in, '-o' + self.base_dir, '-bso0', '-bsp0'])
        return self.uncompressed

    # remove the decompressed dump after processing
    def remove_dump(self):
        self.dump = None
        if self.logger:
            self.logger.info('removing file: %s' % self.uncompressed)
        subprocess.call(['rm',self.uncompressed])