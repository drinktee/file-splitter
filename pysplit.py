#!/usr/bin/env python3

# split text file by lines into chunks
# -------------------------------------------------
# by Patrick Useldinger, uselpa@gmail.com
# -------------------------------------------------
# tested with CPython 2.7.5, 3.3.3 and PyPy 2.2.1
# last modified 12Jan14

#-----------------------------------------------------------------------------------------------------------------------

import math, os, sys, time
PYVERMAJ = sys.version_info.major

LF = '\n' if PYVERMAJ == 2 else ord('\n')  # Python 2/3 compatibility
BUFFSIZE =128*1024*1024

#-----------------------------------------------------------------------------------------------------------------------

class Writer:

    def __init__(self, filename, buffsize, chunks):
        self.filenameroot, self.filenameext = os.path.splitext(filename)
        self.buffsize = buffsize
        self.chunks = chunks
        self.nchunk = len(str(chunks-1))+1
        self.totbytes = 0
        self.f = None
        self.chunk = None

    def openfile(self, chunk):
        filename = self.filenameroot+"."+str(chunk).zfill(self.nchunk)+self.filenameext
        self.f = open(filename, "wb", self.buffsize)
        self.fd = self.f.fileno()
        self.chunk = chunk
        self.bytes = 0
        #print("OP ", chunk, self.f)

    def closefile(self):
        if self.f is not None:
            self.f.close()
            self.totbytes += self.bytes
            print("chunk %i  %i bytes  %i total" % (self.chunk, self.bytes, self.totbytes))
            self.f = None

    def write(self, buffer, chunk):
        #print("WR ", chunk, len(buffer))
        if chunk != self.chunk:
            self.closefile()
            if self.chunk is not None and chunk != self.chunk + 1:
                print("ERROR - non consecutive chunks", self.chunk, chunk)
                sys.exit(8)
            self.openfile(chunk)
        os.write(self.fd, buffer)
        self.bytes += len(buffer)

    def cleanup(self):
        self.closefile()
        if self.chunk is not None:
            for chunk in range(self.chunk + 1, self.chunks):
                self.openfile(chunk)
                self.closefile()
        if self.chunk != self.chunks - 1:
            print("ERROR - created %i chunks, expected %i chunks" % (self.chunk+1, self.chunks))
            sys.exit(8)
        return self.totbytes

#-----------------------------------------------------------------------------------------------------------------------

class Process_Buffer:

    def __init__(self, filename, chunks, chunksize, buffsize):
        self.writer = Writer(filename, buffsize, chunks)
        self.chunk =  self.nbbytesstart  = 0
        self.chunksize = chunksize
        self.nextchunk = chunksize - 1

    def write(self, buffer):
        if buffer:
            self.writer.write(buffer, self.chunk)

    def add(self, buffer):
        nbread = len(buffer)
        if nbread == 0:
            return self.writer.cleanup()
        self.nbbytesend = self.nbbytesstart + nbread
        if self.nextchunk < self.nbbytesend:
            pos = buffer.find(LF, max(0, self.nextchunk-self.nbbytesstart))
            if pos != -1:
                self.write(buffer[:pos+1])
                self.chunk += 1
                self.write(buffer[pos+1:])
                self.nextchunk += self.chunksize
            else:
                self.write(buffer)
        else:
            self.write(buffer)
        self.nbbytesstart = self.nbbytesend
        return None

#-----------------------------------------------------------------------------------------------------------------------

def process_file(filename, chunks):

    filesize = os.path.getsize(filename)
    chunksize = int(math.ceil(filesize * 1.0 / chunks)) # Python 2/3 compatibility
    buffsize = min(chunksize, BUFFSIZE)
    print("splitting into %i chunks of approximately %i bytes using a buffer of %i" % (chunks, chunksize, buffsize))
    process_buffer = Process_Buffer(filename, chunks, chunksize, buffsize)

    f = open(filename, "rb", buffsize)
    fd = f.fileno()
    while True:
        totbytes = process_buffer.add(os.read(fd, buffsize))
        if totbytes is not None:
           break
    f.close()

    if totbytes != filesize:
        print("ERROR - sizes don't match: initial = %i, split total = %i" % (filesize, totbytes))
        sys.exit(8)

#-----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("usage: %s <filename> <chunks>" % (os.path.basename(sys.argv[0])))
        exit(4)

    print(repr(sys.version))

    start_time = time.time()
    process_file(sys.argv[1], int(sys.argv[2]))
    end_time = time.time()
    print("SUCCESS - execution time: %.1f seconds" % (end_time - start_time,))
