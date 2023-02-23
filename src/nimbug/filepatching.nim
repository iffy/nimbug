## This module contains code for copying similar SQLite files
## between computers efficiently.
import std/json
import std/logging
import std/options
import std/sequtils
import std/streams
import std/strformat

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

func `==`*(a, b: ChunkHash): bool {.borrow.}
func `$`*(ch: ChunkHash): string = "ChunkHash-" & $ch.uint32
func `%`*(ch: ChunkHash): JsonNode = newJInt(ch.uint32.int)

func `%`*(pchunk: ChunkPatch): JsonNode =
  result = newJArray()
  result.add newJInt(pchunk.pos)
  result.add newJString(pchunk.data)
func initFromJson*(dst: var ChunkPatch, jsonNode: JsonNode, jsonPath: var string) =
  if jsonNode.kind != JArray:
    raise newException(JsonKindError,
      &"Incorrect JSON kind. Wanted '{JArray}' in '{jsonPath}' but got '{jsonNode.kind}'.")
  dst = (jsonNode[0].getInt, jsonNode[1].getStr())

func `%`*(footer: PatchFooter): JsonNode =
  result = newJArray()
  result.add newJInt(footer.size)
  result.add %(footer.checksum)
func initFromJson*(dst: var PatchFooter, jsonNode: JsonNode, jsonPath: var string) =
  if jsonNode.kind != JArray:
    raise newException(JsonKindError,
      &"Incorrect JSON kind. Wanted '{JArray}' in '{jsonPath}' but got '{jsonNode.kind}'.")
  dst = (jsonNode[0].getInt, jsonNode[1].getInt().ChunkHash)

func isEmpty(cp: ChunkPatch): bool =
  return cp.pos == 0 and cp.data.len == 0

proc combine(a: PreChunkPatch, b: ChunkPatch, maxSize = MAX_CHUNKSIZE) {.raises: [CantCombine, ValueError].} =
  ## Try to combine 2 PreChunkPatches into one to make the number of patches
  ## sent smaller
  if (a.data.len + b.data.len) > maxSize:
    raise CantCombine.newException(&"PreChunkPatch {a.pos} + {b.pos} would be bigger than {maxSize}")
  elif a.pos + a.data.len == b.pos:
    a.data.add b.data.toBytes()
  else:
    raise CantCombine.newException(&"PreChunkPatch {a.pos} is not adjacent to {b.pos}")

# proc combine(a, b: ChunkPatch, maxSize = 1048576): ChunkPatch {.raises: [CantCombine, ValueError].} =
#   ## Try to combine 2 ChunkPatches into one to make the number of patches
#   ## sent smaller
#   if (a.data.len + b.data.len) > maxSize:
#     raise CantCombine.newException(&"ChunkPatch {a.pos} + {b.pos} would be bigger than {maxSize}")
#   elif a.pos + a.data.len == b.pos:
#     (a.pos, a.data & b.data)
#   else:
#     raise CantCombine.newException(&"ChunkPatch {a.pos} is not adjacent to {b.pos}")

proc toPreChunkPatch(x: ChunkPatch): PreChunkPatch =
  new(result)
  result.pos = x.pos
  result.data = x.data.toBytes()

proc toChunkPatch(x: PreChunkPatch): ChunkPatch =
  (x.pos, string.fromBytes(x.data))

iterator hashChunks*(basefile: string, chunksize = DEFAULT_CHUNKSIZE): ChunkHash =
  var fh = openFileStream(basefile, fmRead)
  defer: fh.close()
  while not fh.atEnd:
    yield hashStr(fh.readStr(chunksize)).ChunkHash

proc newPatchMaker*(goalfile: string, chunksize = DEFAULT_CHUNKSIZE): PatchMaker =
  new(result)
  result.fh = openFileStream(goalfile, fmRead)
  result.hasher = newXxh32()
  result.chunksize = chunksize

proc tryCombining*(pm: PatchMaker, cp: ChunkPatch): Option[ChunkPatch] =
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

proc computePatch*(pm: PatchMaker, chunkhash: ChunkHash): Option[ChunkPatch] =
  let loc = pm.fh.getPosition()
  let rchunk = pm.fh.readStr(pm.chunksize)
  pm.hasher.update(rchunk)
  pm.size.inc(rchunk.len)
  let h = hashStr(rchunk)
  if h != chunkhash.uint32:
    let cp = (loc, rchunk)
    result = pm.tryCombining(cp)

iterator remainingPatches*(pm: PatchMaker): ChunkPatch =
  ## Return any additional patches in the case where src is longer than dst
  while not pm.fh.atEnd:
    let pos = pm.fh.getPosition()
    let rchunk = pm.fh.readStr(pm.chunksize)
    pm.hasher.update(rchunk)
    pm.size.inc(rchunk.len)
    let res = pm.tryCombining((pos, rchunk))
    if res.isSome:
      yield res.get()
  if pm.last.isSome:
    yield pm.last.get().toChunkPatch()
    pm.last = none[PreChunkPatch]()

proc footer*(pm: PatchMaker): PatchFooter =
  (pm.size, pm.hasher.digest().ChunkHash)

proc newPatchApplier*(basefile: string, dstfile: string): PatchApplier =
  new(result)
  result.basefh = openFileStream(basefile, fmRead)
  result.dstfh = openFileStream(dstfile, fmWrite)
  result.hasher = newXxh32()

proc apply*(app: PatchApplier, chunk: ChunkPatch) =
  ## Apply a single patch
  let pos = app.dstfh.getPosition()
  debug "MATT app.dstfh.getPosition() = ", $pos
  debug "MATT chunk.pos = ", $chunk.pos
  debug "MATT chunk.data.len = ", $chunk.data.len
  let takeFromBase = chunk.pos - pos
  let fromBase = app.basefh.readStr(takeFromBase)
  app.hasher.update(fromBase)
  app.dstfh.write(fromBase) # TODO: would love to make this faster
  app.hasher.update(chunk.data)
  app.dstfh.write(chunk.data)
  app.basefh.setPosition(app.dstfh.getPosition())

proc apply*(app: PatchApplier, footer: PatchFooter) =
  ## Apply the final footer
  debug "MATT applying footer: ", $footer
  let takeFromBase = footer.size - app.dstfh.getPosition()
  let fromBase = app.basefh.readStr(takeFromBase)
  app.hasher.update(fromBase)
  app.dstfh.write(fromBase) # TODO: would love to make this faster
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

proc `==`*(a, b: ChecksumSet): bool =
  a.chunksize == b.chunksize and a.hashes == b.hashes

proc `==`*(a, b: PatchSet): bool =
  a.chunksize == b.chunksize and a.patches == b.patches and a.footer == b.footer

proc makeChecksumSet*(filename: string, chunksize = DEFAULT_CHUNKSIZE): ChecksumSet =
  new(result)
  result.chunksize = chunksize
  for x in hashChunks(filename, chunksize):
    result.hashes.add(x)

proc makePatchSet*(goalfile: string, cset: ChecksumSet): PatchSet =
  new(result)
  result.chunksize = cset.chunksize
  var pm = newPatchMaker(goalfile)
  for chunkhash in cset.hashes:
    let p = pm.computePatch(chunkhash)
    if p.isSome:
      result.patches.add(p.get())
  result.patches.add(pm.remainingPatches().toSeq)
  result.footer = pm.footer

proc apply*(basefile: string, pset: PatchSet, dstfile: string) =
  var applier = newPatchApplier(basefile, dstfile)
  for patch in pset.patches:
    applier.apply(patch)
  applier.apply(pset.footer)
