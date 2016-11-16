package com.simacan.rtree

import archery._

object Test {
  def main(args: Array[String]): Unit = {

    val ls1 = LineString(Seq(Point(1, 1), Point(2, 1)))
    val ls2 = LineString(Seq(Point(1, 0), Point(1, 2)))
    val ls3 = LineString(Seq(Point(0, 1), Point(0, 2)))
    val ls4 = LineString(Seq(Point(5, 1), Point(6, 1)))
    val ls5 = LineString(Seq(Point(5, 0), Point(5, 2)))
    val ls6 = LineString(Seq(Point(5, 1), Point(5, 2)))
    val ls7 = LineString(Seq(Point(5, 1), Point(6, 1)))
    val ls8 = LineString(Seq(Point(5, 0), Point(5, 2)))
    val ls9 = LineString(Seq(Point(5, 1), Point(5, 2)))


    val tree = RTree(
      Entry(ls1, 1), Entry(ls2, 2), Entry(ls3, 3),
      Entry(ls4, 4), Entry(ls5, 5), Entry(ls6, 6),
      Entry(ls7, 7), Entry(ls8, 8), Entry(ls9, 9))

    println(tree.pretty)

//    val tree = RTree(Entry(ls1, 1))

    val (addr, size, entries) = OffheapRTree.writeOffheap(tree)

    entries.foreach { e =>
      println(OffheapRTree.coordinatesForEntry(e._2).toList)
    }

    println(OffheapRTree.search(addr, Box(5.9f, 0.9f, 10, 10)).toList)
  }
}
