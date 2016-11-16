package com.simacan.rtree

import archery._
import com.vividsolutions.jts.geom.Coordinate

import scala.offheap._
import scala.offheap.{ EmbedArray, data }

object OffheapRTree {

  val FixedPointCoordinateMultiplier = 1000000

  @data class OHBox(var minX: Int, var minY: Int, var maxX: Int, var maxY: Int) {
    def intersects(geom: Box): Boolean =
      minX <= geom.x2 && geom.x <= maxX && minY <= geom.y2 && geom.y <= maxY

  }

  @data class OHCoord(var x: Short, var y: Short)

  /**
    * Created by jeroen on 11/16/16.
    */
  trait OffheapNode extends Any {
    val nodeAddr: Addr

    def headerSize: Long = sizeOf[Byte] + sizeOf[Byte] + sizeOfEmbed[OHBox]

    def nodeType: Byte = Memory.getByte(nodeAddr)

    def nodeNumChildren = Memory.getByte(nodeAddr + sizeOf[Byte])

    def nodeBoxAddr: Addr = nodeAddr + sizeOf[Byte] + sizeOf[Byte]
    def nodeBox: OHBox = OHBox.fromAddr(nodeBoxAddr)

    def search(space: Box): Iterator[OffheapEntry]

    def write(n: Node[Int]): Iterator[(Int, Addr)] = {

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

    def writeTo(addr: Addr, n: Node[Int]) : Iterator[(Int,Addr)] = {
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
      children.filter { item =>
        space.intersects(item._3)
      } .map( e => new OffheapEntry(e._1) )
    }

    def children: Iterator[(Addr, Int, Box)] = {
      val numChildren = nodeNumChildren

      val leafBox = nodeBox

      var addr = nodeAddr + headerSize
      new Iterator[(Addr, Int, Box)] {

        var index = 0

        override def hasNext(): Boolean = {
          index < numChildren
        }

        override def next(): (Addr, Int, Box) = {
          val childAddr = addr

          val value = Memory.getInt(addr)
          addr += sizeOf[Int]

          val numCoords = Memory.getShort(addr)
          addr += sizeOf[Short]

          addr += sizeOf[Short] // Skip box offset

          var minX = Float.MaxValue
          var minY = Float.MaxValue
          var maxX = Float.MinValue
          var maxY = Float.MinValue

          var coordAddr = addr
          var coordIdx = 0
          while (coordIdx < numCoords) {
            val coord: OHCoord = OHCoord.fromAddr(addr)
            addr += sizeOfEmbed[OHCoord]

            val xc = leafBox.minX + (leafBox.maxX - leafBox.minX).toDouble * (coord.x.toDouble / Short.MaxValue)
            val yc = leafBox.minY + (leafBox.maxY - leafBox.minY).toDouble * (coord.y.toDouble / Short.MaxValue)

            val x = math.max(math.min(xc, leafBox.maxX), leafBox.minX)
            val y = math.max(math.min(yc, leafBox.maxY), leafBox.minY)

            minX = math.min(minX, x.toFloat)
            maxX = math.max(maxX, x.toFloat)
            minY = math.min(minY, y.toFloat)
            maxY = math.max(maxY, y.toFloat)

            coordIdx += 1
          }

          val entryBox = Box(minX, minY, maxX, maxY)

          index += 1
          (childAddr, value, entryBox)
        }
      }
    }

    def write(l: Leaf[Int]): Iterator[(Int, Addr)] = {
      var addr = writeNodeHeader(l)

      l.entries.foreach { c =>
        new OffheapEntry(addr).write(c, nodeBoxAddr)
        addr += sizeOfEntry(c)
      }

      // Iterate over the values we just created.
      children.map(tup => (tup._2, tup._1))
    }
  }

  class OffheapBranch(val nodeAddr: Addr) extends AnyVal with OffheapNode {

    override def search(space: Box): Iterator[OffheapEntry] = {
      children.filter { item =>
        item._2.intersects(space)
      }.flatMap { item =>
        OffheapNode.fromAddr(item._1).search(space)
      }
    }

    def children: Iterator[(Addr, OHBox)] = {
      var addr = nodeAddr + 1 // Skip node type

      val numChildren = Memory.getByte(addr)
      addr += sizeOf[Byte]

      addr += sizeOfEmbed[OHBox]  // Skip leaf box.

      new Iterator[(Addr, OHBox)] {
        var index = 0

        override def hasNext(): Boolean = {
          index < numChildren
        }

        override def next(): (Addr, OHBox) = {
          val size = Memory.getInt(addr)
          addr += sizeOf[Int]

          val boxAddr = addr + sizeOf[Byte] + sizeOf[Byte] // Skip type byte and numChildren
          val ohBox: OHBox = OHBox.fromAddr(boxAddr)

          val result = (addr, ohBox)

          addr += size
          index += 1

          result
        }
      }
    }

    def write(b: Branch[Int]): Iterator[(Int, Addr)] = {
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

    def value: Int = Memory.getInt(entryAddr)

    def numCoordinates: Short = Memory.getShort(entryAddr + sizeOf[Int])

    def leafBoxOffset: Short = Memory.getShort(entryAddr + sizeOf[Int] + sizeOf[Short])

    def leafBoxInst : OHBox = OHBox.fromAddr(entryAddr - leafBoxOffset)

    def entryHeaderSize : Long = sizeOf[Int] + sizeOf[Short] + sizeOf[Short]

    def coordinatesForEntry: Iterator[Coordinate] = {
      var addr = entryAddr + entryHeaderSize

      val leafBox: OHBox = leafBoxInst
      val numCoords = numCoordinates

      var coordAddr = addr
      var coordIdx = 0

      new Iterator[Coordinate] {
        override def hasNext: Boolean = coordIdx < numCoords

        override def next(): Coordinate = {

          val coord: OHCoord = OHCoord.fromAddr(addr)
          addr += sizeOfEmbed[OHCoord]

          val x = (leafBox.minX + (leafBox.maxX - leafBox.minX).toDouble * (coord.x.toDouble / Short.MaxValue)) / FixedPointCoordinateMultiplier
          val y = (leafBox.minY + (leafBox.maxY - leafBox.minY).toDouble * (coord.y.toDouble / Short.MaxValue)) / FixedPointCoordinateMultiplier

          val result = new Coordinate(x, y)

          coordIdx += 1

          result
        }
      }
    }

    def coordinateAt(coordIdx: Int): Coordinate = {
      val addr = entryAddr + entryHeaderSize

      val leafBox: OHBox = leafBoxInst
      val coord: OHCoord = OHCoord.fromAddr(addr + (coordIdx * sizeOfEmbed[OHCoord]))

      val x = (leafBox.minX + (leafBox.maxX - leafBox.minX).toDouble * (coord.x.toDouble / Short.MaxValue)) / FixedPointCoordinateMultiplier
      val y = (leafBox.minY + (leafBox.maxY - leafBox.minY).toDouble * (coord.y.toDouble / Short.MaxValue)) / FixedPointCoordinateMultiplier

      new Coordinate(x, y)
    }

    def write(e: Entry[Int], boxAddr: Addr): Unit = {
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

          val dx = leafBox.maxX - leafBox.minX
          val dy = leafBox.maxY - leafBox.minY

          ls.coords.foreach { c =>

            val x: Short = (((c.x.toDouble * FixedPointCoordinateMultiplier - leafBox.minX) / dx) * Short.MaxValue).toShort
            val y: Short = (((c.y.toDouble * FixedPointCoordinateMultiplier - leafBox.minY) / dy) * Short.MaxValue).toShort

            val ohCoord: OHCoord = OHCoord.fromAddr(addr)
            ohCoord.x = x
            ohCoord.y = y
            addr += sizeOfEmbed[OHCoord]
          }
      }
    }

    override def toString = {
      value + "@" + entryAddr
    }
  }

  private[this] val BranchNode: Byte = 1
  private[this] val LeafNode: Byte = 2

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

  def writeOffheap(tree: RTree[Int]): (Addr, Long, Iterator[(Int, Addr)]) = {

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
}