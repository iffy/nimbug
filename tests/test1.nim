import std/json
import std/logging
import std/options
import std/os
import std/random
import std/sequtils
import std/sha1
import std/streams
import std/strformat
import std/strutils
import std/unittest

import stew/byteutils
import xxhash

const DEFAULT_CHUNKSIZE = 4096 # SQLite's default page size
const DEFAULT_SEED: int = 273892837
const MAX_CHUNKSIZE = DEFAULT_CHUNKSIZE * 20

template hashStr(x: string): uint32 =
  XXH32(x, DEFAULT_SEED)

#----------------------------------------------------------------
# Streaming interface
#----------------------------------------------------------------
type
  PatchFailed* = object of CatchableError
  CantCombine* = object of CatchableError

  ChunkHash* = distinct uint32

  PatchMaker* = ref object
    size: int
    fh: FileStream
    hasher: Xxh32State
    chunksize: int
    last: Option[PreChunkPatch]

  PreChunkPatch* = ref object
    pos: int
    data: seq[byte]

  ChunkPatch* = tuple
    pos: int
    data: string
  
  PatchFooter* = tuple
    size: int
    checksum: ChunkHash
  
  PatchApplier* = ref object
    hasher: Xxh32State
    basefh: FileStream
    dstfh: FileStream

func `$`(ch: ChunkHash): string = "ChunkHash-" & $ch.uint32
proc `$`(cp: ChunkPatch): string =
  &"ChunkPatch(size={cp.data.len} @{cp.pos})"

proc combine(a: PreChunkPatch, b: ChunkPatch, maxSize = MAX_CHUNKSIZE) {.raises: [CantCombine, ValueError].} =
  ## Try to combine 2 PreChunkPatches into one to make the number of patches
  ## sent smaller
  echo &"  combine(size={a.data.len} @{a.pos} w/ size={b.data.len} @{b.pos})"
  if (a.data.len + b.data.len) > maxSize:
    raise CantCombine.newException(&"PreChunkPatch {a.pos} + {b.pos} would be bigger than {maxSize}")
  elif a.pos + a.data.len == b.pos:
    a.data.add b.data.toBytes()
    echo &"    combined into: size={a.data.len} @{a.pos}"
  else:
    raise CantCombine.newException(&"PreChunkPatch {a.pos} is not adjacent to {b.pos}")

proc toPreChunkPatch(x: ChunkPatch): PreChunkPatch =
  new(result)
  result.pos = x.pos
  result.data = x.data.toBytes()

proc toChunkPatch(x: PreChunkPatch): ChunkPatch =
  (x.pos, string.fromBytes(x.data))

iterator hashChunks(basefile: string, chunksize = DEFAULT_CHUNKSIZE): ChunkHash =
  var fh = openFileStream(basefile, fmRead)
  echo "hashChunks ", basefile, " ", $chunksize
  defer: fh.close()
  while not fh.atEnd:
    let data = fh.readStr(chunksize)
    let h = hashStr(data).ChunkHash
    echo "  read ", $data.len, " ", $h
    yield h

proc newPatchMaker(goalfile: string, chunksize = DEFAULT_CHUNKSIZE): PatchMaker =
  new(result)
  result.fh = openFileStream(goalfile, fmRead)
  result.hasher = newXxh32()
  result.chunksize = chunksize

proc tryCombining(pm: PatchMaker, cp: ChunkPatch): Option[ChunkPatch] =
  ## Attempt to combine the given patch with the last encountered patch
  ## If able to combine, return None
  ## If not, the earlier patch will be returned and the given patch will
  ## be stored for future calls to this function.
  if pm.last.isNone:
    pm.last = some(cp.toPreChunkPatch())
  else:
    let last = pm.last.get()
    try:
      combine(last, cp)
    except CantCombine:
      result = some(last.toChunkPatch())
      pm.last = some(cp.toPreChunkPatch())

proc computePatch(pm: PatchMaker, chunkhash: ChunkHash): Option[ChunkPatch] =
  echo "computePatch ", $chunkhash
  let loc = pm.fh.getPosition()
  echo "  loc: ", $loc
  let rchunk = pm.fh.readStr(pm.chunksize)
  echo "  rchunk.len: ", $rchunk.len
  pm.hasher.update(rchunk)
  pm.size.inc(rchunk.len)
  echo "  pm.size now ", $pm.size
  let h = hashStr(rchunk)
  if h != chunkhash.uint32:
    let cp = (loc, rchunk)
    result = pm.tryCombining(cp)
  echo "  res -> ", $result

iterator remainingPatches(pm: PatchMaker): ChunkPatch =
  ## Return any additional patches in the case where src is longer than dst
  echo "remainingPatches ..."
  while not pm.fh.atEnd:
    let loc = pm.fh.getPosition()
    let rchunk = pm.fh.readStr(pm.chunksize)
    echo "  loc: ", $loc
    echo "  rchunk.len: ", $rchunk.len
    pm.hasher.update(rchunk)
    pm.size.inc(rchunk.len)
    echo "  pm.size now ", $pm.size
    let res = pm.tryCombining((loc, rchunk))
    if res.isSome:
      echo "  yield -> ", $res.get()
      yield res.get()
  if pm.last.isSome:
    echo "  final patch -> ", pm.last.get().toChunkPatch()
    yield pm.last.get().toChunkPatch()
    pm.last = none[PreChunkPatch]()

proc footer(pm: PatchMaker): PatchFooter =
  (pm.size, pm.hasher.digest().ChunkHash)

proc logPos(pa: PatchApplier) =
  echo "  dst POS ", $pa.dstfh.getPosition()
  echo "  bas POS ", $pa.basefh.getPosition()

proc newPatchApplier(basefile: string, dstfile: string): PatchApplier =
  echo "newPatchApplier"
  new(result)
  result.basefh = openFileStream(basefile, fmRead)
  result.dstfh = openFileStream(dstfile, fmWrite)
  result.hasher = newXxh32()
  result.logPos()

proc apply(app: PatchApplier, chunk: ChunkPatch) =
  ## Apply a single patch
  echo "apply(chunk)"
  echo "chunk.pos = ", $chunk.pos
  echo "chunk.len = ", $chunk.data.len
  app.logPos()
  let pos = app.dstfh.getPosition()
  let takeFromBase = chunk.pos - pos
  echo "chunk.pos - pos: ", $takeFromBase
  let fromBase = app.basefh.readStr(takeFromBase)
  echo "basefh.readStr"
  app.logPos()
  app.hasher.update(fromBase)
  app.dstfh.write(fromBase)
  echo "dstfh WROTE fromBase: ", $fromBase.len
  app.logPos()
  app.hasher.update(chunk.data)
  app.dstfh.write(chunk.data)
  echo "dstfh WROTE chunk.data: ", $chunk.data.len
  app.logPos()
  let newpos = app.dstfh.getPosition()
  app.basefh.setPosition(newpos)
  echo "basefh.setPosition"
  app.logPos()

proc apply(app: PatchApplier, footer: PatchFooter) =
  ## Apply the final footer
  echo "apply(footer)"
  echo "footer.size = ", $footer.size
  app.logPos()
  let takeFromBase = footer.size - app.dstfh.getPosition()
  let fromBase = app.basefh.readStr(takeFromBase)
  app.hasher.update(fromBase)
  app.dstfh.write(fromBase)
  echo "dstfh WROTE fromBase: ", $fromBase.len
  app.logPos()
  app.dstfh.close()
  app.basefh.close()
  if app.hasher.digest() != footer.checksum.uint32:
    raise PatchFailed.newException("File checksums didn't match")
  

#----------------------------------------------------------------
# One shot interface
#----------------------------------------------------------------
type
  ChecksumSet* = ref object
    chunksize*: int
    hashes*: seq[ChunkHash]
  
  PatchSet* = ref object
    chunksize*: int
    patches*: seq[ChunkPatch]
    footer*: PatchFooter

proc makeChecksumSet(filename: string, chunksize = DEFAULT_CHUNKSIZE): ChecksumSet =
  echo "makeChecksumSet ", $filename
  new(result)
  result.chunksize = chunksize
  for x in hashChunks(filename, chunksize):
    result.hashes.add(x)

proc makePatchSet(goalfile: string, cset: ChecksumSet): PatchSet =
  echo "makePatchSet"
  new(result)
  result.chunksize = cset.chunksize
  var pm = newPatchMaker(goalfile)
  for chunkhash in cset.hashes:
    let p = pm.computePatch(chunkhash)
    if p.isSome:
      result.patches.add(p.get())
  result.patches.add(pm.remainingPatches().toSeq)
  result.footer = pm.footer

proc apply(basefile: string, pset: PatchSet, dstfile: string) =
  echo "apply patchset ", basefile, " ", dstfile
  var applier = newPatchApplier(basefile, dstfile)
  for i, patch in pset.patches:
    echo "=== patch ", $i
    applier.apply(patch)
  echo "=== footer"
  applier.apply(pset.footer)


randomize()

proc tmpDir(): string =
  result = os.getTempDir() / &"nimbugtest{random.rand(10000000)}"
  result.createDir()

template withinTmpDir(body:untyped):untyped =
  let
    tmp = tmpDir()
    olddir = getCurrentDir()
  setCurrentDir(tmp)
  body
  setCurrentDir(olddir)
  try:
    tmp.removeDir()
  except:
    echo "WARNING: failed to remove temporary test directory: ", getCurrentExceptionMsg()

proc fileHash(path: string): string =
  return "sha1:" & ($secureHashFile(path)).toLowerAscii()

proc randomString(size: int): string =
  for i in 0..<size:
    result.add(chr(rand(255)))

proc copyByPatch(goal, base, dst: string) =
  echo "\nmaking checksum set"
  var cset = makeChecksumSet(base)
  echo "checkset: ", $cset.hashes.len, " ", $cset[]
  echo "\nmaking patch set"
  var pset = makePatchSet(goal, cset)
  echo "patchset: ", $pset.patches.len
  echo "\napplying patch set"
  apply(base, pset, dst)

proc jsonRoundtrip[T](x: T): T =
  let serialized = $(%*(x))
  return serialized.parseJson.to(T)

test "streaming":
  withinTmpDir:
    let goal = "goal.txt"
    let base = "base.txt"
    let dst = "dst.txt"
    writeFile(goal, "a".repeat(10000))
    writeFile(base, "a".repeat(1000) & "." & "a".repeat(7000))
    var pm = newPatchMaker(goal)
    var patches: seq[ChunkPatch]
    for chunkhash in hashChunks(base):
      check chunkhash == jsonRoundtrip(chunkhash)
      let p = pm.computePatch(chunkhash)
      if p.isSome:
        patches.add(p.get())
    patches.add(pm.remainingPatches.toSeq)
    for p in patches:
      check p == jsonRoundtrip(p)
    let footer = pm.footer
    check footer == jsonRoundtrip(footer)
    var applier = newPatchApplier(base, dst)
    for patch in patches:
      applier.apply(patch)
    applier.apply(footer)
    check fileHash(goal) == fileHash(dst)
    check getFileSize(dst) == 10000

test "oneshot":
  withinTmpDir:
    let goal = "a.txt"
    let base = "b.txt"
    let dst = "d.txt"
    writeFile(goal, "a".repeat(4500))
    writeFile(base, "b".repeat(5000))
    var cset = makeChecksumSet(base)
    check cset == jsonRoundtrip(cset)
    var pset = makePatchSet(goal, cset)
    check pset == jsonRoundtrip(pset)
    apply(base, pset, dst)
    check fileHash(goal) == fileHash(dst)
    check getFileSize(dst) == 4500

test "checksum":
  withinTmpDir:
    let goal = "a.txt"
    let base = "b.txt"
    let dst = "d.txt"
    writeFile(goal, "a".repeat(4500) & "b".repeat(5000))
    writeFile(base, "a".repeat(4500) & "c".repeat(300))
    var cset = makeChecksumSet(base)
    var pset = makePatchSet(goal, cset)
    writeFile(base, "y".repeat(4500) & "c".repeat(300))
    expect PatchFailed:
      apply(base, pset, dst)

test "random grow":
  withinTmpDir:
    let goal = "goal.txt"
    let base = "base.txt"
    let dst = "dst.txt"
    randomize(1234)
    writeFile(goal, "a".repeat(5000) & randomString(8000))
    writeFile(base, "a".repeat(5000) & randomString(6000))
    copyByPatch(goal, base, dst)
    check fileHash(goal) == fileHash(dst)
    check getFileSize(dst) == 5000 + 8000

test "random shrink":
  withinTmpDir:
    let goal = "goal.txt"
    let base = "base.txt"
    let dst = "dst.txt"
    randomize(1234)
    writeFile(goal, "a".repeat(5000) & randomString(6000))
    writeFile(base, "a".repeat(5000) & randomString(9000))
    copyByPatch(goal, base, dst)
    check fileHash(goal) == fileHash(dst)
    check getFileSize(dst) == 5000 + 6000