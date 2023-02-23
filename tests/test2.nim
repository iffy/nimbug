import std/os
import std/streams
import std/unittest

let goal = currentSourcePath().parentDir() / "goal.txt"
let base = currentSourcePath().parentDir() / "base.txt"

test "grow":
  let goalFile = "_test"/"goal.txt"
  goalFile.parentDir.createDir()
  writeFile(goalFile, readFile(goal))
  let fh = openFileStream(goalFile, fmRead)
  while not fh.atEnd:
    let loc = fh.getPosition()
    echo "loc: ", $loc
    let chunk = fh.readStr(4096)
    echo "chunklen: ", $chunk.len
  check fh.getPosition() == 13000