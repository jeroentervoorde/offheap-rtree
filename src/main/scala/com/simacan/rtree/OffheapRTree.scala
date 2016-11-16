package com.simacan.rtree

import archery._
import com.vividsolutions.jts.geom.Coordinate

import scala.offheap._
import scala.offheap.{ EmbedArray, data }

object OffheapRTree {

  val FixedPointCoordinateMultiplier = 1000000
  private[this] val BranchNode: Byte = 1
  private[this] val LeafNode: Byte = 2

  @data class OHBox(var minX: Int, var minY: Int, var maxX: Int, var maxY: Int) {
    def intersects(geom: Box): Boolean =
      minX <= geom.x2 && geom.x <= maxX && minY <= geom.y2 && geom.y <= maxY
  }

  @data class OHCoord(var x: Short, var y: Short)

  trait OffheapNode extends Any {
    val nodeAddr: Addr

    private[rtree] def headerSize: Long = sizeOf[Byte] + sizeOf[Byte] + sizeOfEmbed[OHBox]
    private[rtree] def nodeBoxAddr: Addr = nodeAddr + sizeOf[Byte] + sizeOf[Byte]

    def nodeType: Byte = Memory.getByte(nodeAddr)
    def nodeNumChildren = Memory.getByte(nodeAddr + sizeOf[Byte])
    def nodeBox: OHBox = OHBox.fromAddr(nodeBoxAddr)

    def search(space: Box): Iterator[OffheapEntry]

    def write(n: Node[Int]): Iterator[OffheapEntry] = {

      n match {
        case l: Leaf[Int] => new OffheapLeaf(nodeAddr).write(l)
        case b: Branch[Int] => new OffheapBranch(nodeAddr).write(b)
      }
    }

    protected[this] def writeNodeHeader(n: Node[Int]): Addr = {
      var addr = nodeAddr

      val nodeType = n match {
        case _: Branch[Int] => BranchNode
        case _: Leaf[Int] => LeafNode
      }

      Memory.putByte(addr, nodeType)
      addr += sizeOf[Byte]

      Memory.putByte(addr, n.children.size.toByte)
      addr += sizeOf[Byte]

      val boxAddr = addr
      val box: OHBox = OHBox.fromAddr(boxAddr)
      box.minX = (n.box.x.toDouble * FixedPointCoordinateMultiplier).floor.toInt
      box.minY = (n.box.y.toDouble * FixedPointCoordinateMultiplier).floor.toInt
      box.maxX = (n.box.x2.toDouble * FixedPointCoordinateMultiplier).ceil.toInt
      box.maxY = (n.box.y2.toDouble * FixedPointCoordinateMultiplier).ceil.toInt
      addr += sizeOfEmbed[OHBox]

      addr
    }
  }

  object OffheapNode {

    def writeTo(addr: Addr, n: Node[Int]) : Iterator[OffheapEntry] = {
      val node = n match {
        case l: Leaf[Int] => new OffheapLeaf(addr)
        case b: Branch[Int] => new OffheapBranch(addr)
      }

      node.write(n)
    }

    def fromAddr(addr: Addr) : OffheapNode = {
      val nodeType = Memory.getByte(addr)
      nodeType match {
        case LeafNode => new OffheapLeaf(addr)
        case BranchNode => new OffheapBranch(addr)
      }
    }
  }

  class OffheapLeaf(val nodeAddr: Addr) extends AnyVal with OffheapNode {

    override def search(space: Box): Iterator[OffheapEntry] = {
      children.filter(item => space.intersects(item.box))
    }

    def children: Iterator[OffheapEntry] = {
      val numChildren = nodeNumChildren

      new Iterator[OffheapEntry] {
        var addr = nodeAddr + headerSize
        var index = 0

        override def hasNext(): Boolean = {
          index < numChildren
        }

        override def next(): OffheapEntry = {
          index += 1
          val entry = new OffheapEntry(addr)
          addr += entry.sizeInBytes
          entry
        }
      }
    }

    private[rtree] def write(l: Leaf[Int]): Iterator[OffheapEntry] = {
      var addr = writeNodeHeader(l)

      l.entries.foreach { c =>
        new OffheapEntry(addr).write(c, nodeBoxAddr)
        addr += sizeOfEntry(c)
      }

      // Iterate over the values we just created.
      children
    }
  }

  class OffheapBranch(val nodeAddr: Addr) extends AnyVal with OffheapNode {

    override def search(space: Box): Iterator[OffheapEntry] = {
      children.filter(_.nodeBox.intersects(space)).flatMap(_.search(space))
    }

    def children: Iterator[OffheapNode] = {
      var addr = nodeAddr + headerSize

      val numChildren = nodeNumChildren

      new Iterator[OffheapNode] {
        var index = 0

        override def hasNext(): Boolean = {
          index < numChildren
        }

        override def next(): OffheapNode = {
          val size = Memory.getInt(addr)
          addr += sizeOf[Int]

          val node = OffheapNode.fromAddr(addr)
          val result = node

          addr += size
          index += 1

          result
        }
      }
    }

    private[rtree] def write(b: Branch[Int]): Iterator[OffheapEntry] = {
      var addr = writeNodeHeader(b)

      val childIterators = b.children.map { c =>
        val size = sizeOfNode(c).toInt
        Memory.putInt(addr, size)
        addr += sizeOf[Int]

        val entries = OffheapNode.writeTo(addr, c)
        addr += size

        entries
      }

      childIterators.iterator.flatten
    }
  }

  class OffheapEntry(val entryAddr: Addr) extends AnyVal {

    private[rtree] def sizeInBytes = entryHeaderSize + numCoordinates * sizeOfEmbed[OHCoord]
    private[this] def leafBoxOffset: Short = Memory.getShort(entryAddr + sizeOf[Int] + sizeOf[Short])
    private[rtree] def leafBoxInst : OHBox = OHBox.fromAddr(entryAddr - leafBoxOffset)
    private[rtree] def entryHeaderSize : Long = sizeOf[Int] + sizeOf[Short] + sizeOf[Short]

    def value: Int = Memory.getInt(entryAddr)

    def numCoordinates: Short = Memory.getShort(entryAddr + sizeOf[Int])

    def box : Box = {
      val leafBox = leafBoxInst
      val numCoords = numCoordinates

      var minX = Float.MaxValue
      var minY = Float.MaxValue
      var maxX = Float.MinValue
      var maxY = Float.MinValue

      //var coordAddr = addr + entry.entryHeaderSize
      var coordIdx = 0
      while (coordIdx < numCoords) {
        val coord: OHCoord = relativeCoordAt(coordIdx)

        val x = convertXFromRelativeToFixed(leafBox, coord.x)
        val y = convertYFromRelativeToFixed(leafBox, coord.y)

        minX = math.min(minX, x.toFloat)
        maxX = math.max(maxX, x.toFloat)
        minY = math.min(minY, y.toFloat)
        maxY = math.max(maxY, y.toFloat)

        coordIdx += 1
      }

      val minXcapped = math.max(minX, leafBox.minX)  // Make sure x,y are in the box
      val minYcapped = math.max(minY, leafBox.minY)
      val maxXcapped = math.min(maxX, leafBox.maxX)  // Make sure x,y are in the box
      val maxYcapped = math.min(maxY, leafBox.maxY)

      Box(minXcapped, minYcapped, maxXcapped, maxYcapped)
    }

    // Coordinate at index
    def coordinateAt(coordIdx: Int): Coordinate = {
      val addr = entryAddr + entryHeaderSize

      val leafBox: OHBox = leafBoxInst
      val coord: OHCoord = OHCoord.fromAddr(addr + (coordIdx * sizeOfEmbed[OHCoord]))

      convertEntryCoordinate(leafBox, coord)
    }

    // Iterate over all coordinates
    def coordinatesForEntry: Iterator[Coordinate] = {
      var addr = entryAddr + entryHeaderSize

      val leafBox: OHBox = leafBoxInst
      val numCoords = numCoordinates

      new Iterator[Coordinate] {
        private[this] var coordAddr = addr
        private[this] var coordIdx = 0

        override def hasNext: Boolean = coordIdx < numCoords

        override def next(): Coordinate = {

          val coord: OHCoord = OHCoord.fromAddr(addr)
          addr += sizeOfEmbed[OHCoord]

          coordIdx += 1
          convertEntryCoordinate(leafBox, coord)
        }
      }
    }

    private[rtree] def relativeCoordAt(coordIdx: Int): OHCoord = {
      val addr = entryAddr + entryHeaderSize + coordIdx * sizeOfEmbed[OHCoord]
      OHCoord.fromAddr(addr)
    }

    private[rtree] def write(e: Entry[Int], boxAddr: Addr): Unit = {
      var addr = entryAddr
      Memory.putInt(addr, e.value)
      addr += sizeOf[Int]

      e.geom match {
        case ls: LineString =>
          Memory.putShort(addr, ls.coords.size.toShort)
          addr += sizeOf[Short]

          if (entryAddr - boxAddr > Short.MaxValue) {
            throw new RuntimeException(s"Cannot address box position (${entryAddr - boxAddr} > ${Short.MaxValue})")
          }

          Memory.putShort(addr, (entryAddr - boxAddr).toShort)
          addr += sizeOf[Short]

          val leafBox: OHBox = OHBox.fromAddr(boxAddr)

          ls.coords.foreach { c =>
            val x: Short = convertXToRelative(leafBox, c.x)
            val y: Short = convertYToRelative(leafBox, c.y)

            val ohCoord: OHCoord = OHCoord.fromAddr(addr)
            ohCoord.x = x
            ohCoord.y = y
            addr += sizeOfEmbed[OHCoord]
          }
      }
    }

    @inline private[rtree] def convertXToRelative(leafBox: OHBox, x: Float) : Short = {
      val dx = leafBox.maxX - leafBox.minX
      (((x.toDouble * FixedPointCoordinateMultiplier - leafBox.minX) / dx) * Short.MaxValue).toShort
    }

    @inline private[rtree] def convertYToRelative(leafBox: OHBox, y: Float) : Short = {
      val dy = leafBox.maxY - leafBox.minY
      (((y.toDouble * FixedPointCoordinateMultiplier - leafBox.minY) / dy) * Short.MaxValue).toShort
    }

    @inline private[rtree] def convertXFromRelativeToFixed(leafBox: OHBox, x: Short) : Int = {
      (leafBox.minX + (leafBox.maxX - leafBox.minX).toDouble * (x.toDouble / Short.MaxValue)).toInt
    }

    @inline private[rtree] def convertYFromRelativeToFixed(leafBox: OHBox, y: Short) : Int = {
      (leafBox.minY + (leafBox.maxY - leafBox.minY).toDouble * (y.toDouble / Short.MaxValue)).toInt
    }

    @inline private[rtree] def convertEntryCoordinate(leafBox: OHBox, coord: OHCoord) : Coordinate = {
      val x = convertXFromRelativeToFixed(leafBox, coord.x).toDouble / FixedPointCoordinateMultiplier
      val y = convertYFromRelativeToFixed(leafBox, coord.y).toDouble / FixedPointCoordinateMultiplier
      new Coordinate(x, y)
    }

    override def toString = {
      value + "@" + entryAddr
    }
  }

  def writeOffheap(tree: RTree[Int]): (Addr, Long, Iterator[OffheapEntry]) = {

    val size = sizeOfNode(tree.root)
    val addr = malloc.allocate(size)
    val entries = OffheapNode.writeTo(addr, tree.root)
    (addr, size, entries)
  }

  def search(atAddr: Addr, space: Box): Iterator[OffheapEntry] = {

    val spaceFixed = Box(
      space.x * FixedPointCoordinateMultiplier.floor,
      space.y * FixedPointCoordinateMultiplier.floor,
      space.x2 * FixedPointCoordinateMultiplier.ceil,
      space.y2 * FixedPointCoordinateMultiplier.ceil
    )

    OffheapNode.fromAddr(atAddr).search(spaceFixed)
  }

  private[this] def sizeOfEntry(e: Entry[Int]): Long = {
    val g = e.geom match {
      case ls: LineString =>
        sizeOf[Short] + sizeOf[Short] + ls.coords.size * sizeOfEmbed[OHCoord] // box offset, num coords, coords
    }

    sizeOf[Int] + g // value + geom
  }

  private[this] def sizeOfNode(n: Node[Int]): Long = {
    n match {
      case b: Branch[Int] =>
        sizeOf[Byte] + sizeOf[Byte] + sizeOfEmbed[OHBox] + b.children.map(c => sizeOf[Int] + sizeOfNode(c)).sum
      case l: Leaf[Int] =>
        sizeOf[Byte] + sizeOf[Byte] + sizeOfEmbed[OHBox] + l.children.map(sizeOfEntry).sum
    }
  }
}