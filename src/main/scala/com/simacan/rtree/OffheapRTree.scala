package com.simacan.rtree

import archery._

import scala.offheap._
import scala.offheap.{EmbedArray, data}

object OffheapRTree {

  @data class OHBox(var minX: Float, var minY: Float, var maxX: Float, var maxY: Float) {
    def intersects(geom: Box): Boolean =
      minX <= geom.x2 && geom.x <= maxX && minY <= geom.y2 && geom.y <= maxY

  }

  @data class OHCoord(var x: Float,  var y: Float)

  //@data class OHNode(var nodeType: Byte, var numChildren: Int, val box: OHBox)

  private[this] val BranchNode : Byte = 1
  private[this] val LeafNode : Byte = 2

  private[this] def sizeOfEntry(e: Entry[Int]) : Long = {
    val g = e.geom match {
      case ls: LineString =>
        sizeOf[Int] + ls.coords.size * sizeOfEmbed[OHCoord]
    }

    sizeOf[Int] + g
  }

  private[this] def entryToOffheap(atAddr: Long, e: Entry[Int]) : Unit = {
    var addr = atAddr
    Memory.putInt(addr, e.value)
    addr += sizeOf[Int]

    e.geom match {
      case ls: LineString =>
        Memory.putInt(addr, ls.coords.size)
        addr += sizeOf[Int]

        ls.coords.foreach { c =>
          val ohCoord : OHCoord = OHCoord.fromAddr(addr)
          ohCoord.x = c.x
          ohCoord.y = c.y
          addr += sizeOfEmbed[OHCoord]
        }
    }
  }

  private[this] def sizeOfNode(n: Node[Int]): Long = {
    n match {
      case b: Branch[Int] =>
        sizeOf[Byte] + sizeOf[Int] + sizeOfEmbed[OHBox] + b.children.map(c => sizeOf[Int] + sizeOfNode(c)).sum
      case l: Leaf[Int] =>
        sizeOf[Byte] + sizeOf[Int] + sizeOfEmbed[OHBox] + l.children.map(sizeOfEntry).sum
    }
  }

  private[this] def nodeToOffheap(atAddr: Long, n: Node[Int]) : Seq[(Int, Addr)] = {
    var addr = atAddr

    val nodeType = n match {
      case _: Branch[Int] => BranchNode
      case _: Leaf[Int] => LeafNode
    }

    Memory.putByte(addr, nodeType)
    addr += sizeOf[Byte]

    Memory.putInt(addr, n.children.size)
    addr += sizeOf[Int]

    val box : OHBox = OHBox.fromAddr(addr)
    box.minX = n.box.x
    box.minY = n.box.y
    box.maxX = n.box.x2
    box.maxY = n.box.y2
    addr += sizeOfEmbed[OHBox]

    n match {
      case b: Branch[Int] => branchToOffheap(addr, b)
      case l: Leaf[Int] => leafToOffheap(addr, l)
    }
  }

  private[this] def branchToOffheap(atAddr: Addr, b: Branch[Int]) : Seq[(Int, Addr)] = {
    var addr = atAddr

    b.children.flatMap { c =>
      val size = sizeOfNode(c).toInt
      Memory.putInt(addr, size)
      addr += sizeOf[Int]

      val entries = nodeToOffheap(addr, c)
      addr += size

      entries
    }
  }

  private[this] def leafToOffheap(atAddr: Addr, l: Leaf[Int]) : Seq[(Int, Addr)] = {
    var addr = atAddr

    l.entries.map { c =>
      entryToOffheap(addr, c)
      addr += sizeOfEntry(c)

      (c.value, addr)
    }
  }

  def writeOffheap(tree: RTree[Int]) : (Addr, Long, Seq[(Int, Addr)]) = {

    val size = sizeOfNode(tree.root)
    val addr = malloc.allocate(size)
    val entries = nodeToOffheap(addr, tree.root)
    (addr, size, entries)
  }

  def search(atAddr: Addr, space: Box) : Iterator[Int] = {

    var addr = atAddr
    val nodeType = Memory.getByte(addr)
    addr += sizeOf[Byte]

    val numChildren = Memory.getInt(addr)
    addr += sizeOf[Int]

    val ohBox : OHBox = OHBox.fromAddr(addr)
    addr += sizeOfEmbed[OHBox]

    val box = Box(ohBox.minX, ohBox.minY, ohBox.maxX, ohBox.maxY)

    if (numChildren == 0 || !box.intersects(space)) {
      Iterator.empty
    } else {
      nodeType match {
        case LeafNode =>
          val children = new Iterator[(Int, Box)] {

            var index = 0

            override def hasNext(): Boolean = {
              index < numChildren
            }

            override def next(): (Int, Box) = {
              val value = Memory.getInt(addr)
              addr += sizeOf[Int]

              val numCoords = Memory.getInt(addr)
              addr += sizeOf[Int]

              var minX = Float.MaxValue
              var minY = Float.MaxValue
              var maxX = Float.MinValue
              var maxY = Float.MinValue

              var coordAddr = addr
              var coordIdx = 0
              while(coordIdx < numCoords) {
                val coord : OHCoord = OHCoord.fromAddr(addr)
                addr += sizeOfEmbed[OHCoord]

                minX = math.min(minX, coord.x)
                maxX = math.max(maxX, coord.x)
                minY = math.min(minY, coord.y)
                maxY = math.max(maxY, coord.y)

                coordIdx += 1
              }

              val entryBox = Box(minX, minY, maxX, maxY)

              index += 1
              (value, entryBox)
            }
          }

          children.filter(item => space.intersects(item._2)).map(_._1)

        case BranchNode =>

          val children = new Iterator[(Addr, OHBox)] {
            var index = 0

            override def hasNext(): Boolean = {
              index < numChildren
            }

            override def next(): (Addr, OHBox) = {
              val size = Memory.getInt(addr)
              addr += sizeOf[Int]

              val boxAddr = addr + sizeOf[Byte] + sizeOf[Int] // Skip type byte and numChildren
              val ohBox : OHBox = OHBox.fromAddr(boxAddr)

              val result = (addr, ohBox)

              addr += size
              index += 1

              result
            }
          }

          children.filter(item => item._2.intersects(space)).flatMap(item => search(item._1, space))
      }
    }
  }
}
