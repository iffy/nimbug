import std/streams

writeFile("foo.txt", "hi")
let s = openFileStream("foo.txt", fmRead)
doAssert s.getPosition() == 0
discard s.readStr(3)
doAssert s.getPosition() == 2
discard s.atEnd()
doAssert s.getPosition() == 2
echo $s.getPosition()