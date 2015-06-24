/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.incrementalML.classification

import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.incrementalML.classification.Metrics.AttributeType
import org.apache.flink.streaming.incrementalML.classification.Metrics.AttributeType.AttributeType

import scala.collection.mutable


object DecisionTreeModel
  extends Serializable {

  var decisionTree: mutable.Map[Int, DTNode] = mutable.HashMap[Int, DTNode]()

  def createRootOfTheTree: Unit = {
    decisionTree.+=((0, DTNode(true, true, 0, None)))
  }

  /** Sorts the given data point to one of the tree's leaves.
    *
    * @param dataPointFeatures The features of the data point to be sorted with the Decision
    *                          tree as-is till now
    * @return The leaf, that this was sorted to
    */
  def classifyDataPointToLeaf(dataPointFeatures: Vector): Int = {
    var leaf = 0

    //classify data point and return leaf id
    var currentNode = decisionTree.get(0).get

    while (!currentNode.isLeaf) {
      var tempChildrenList = currentNode.children.getOrElse(throw new RuntimeException(s"" +
        s"Node:$currentNode is not a " +
        s"leaf but has no children."))

      currentNode.splitAttributeType match {
        case Some(AttributeType.Numerical) => {
          //left hand side of the tree for values <=
          if (dataPointFeatures(currentNode.splitAttribute.get) <= currentNode.
            attributeSplitValue.get.head) {
            val temp = tempChildrenList.getOrElse(0, throw new RuntimeException
            ("Left Hand Side branch doesn't exist-----1"))
            currentNode = decisionTree.getOrElse(temp, throw new RuntimeException
            ("Left Hand Side branch doesn't exist-----1"))
            if (currentNode.children != None) {
              tempChildrenList = currentNode.children.get
            }
            leaf = currentNode.nodeId
          }
          else {
            //right hand side of the tree for values >
            val temp = tempChildrenList.getOrElse(1.0, throw new RuntimeException(
              "Right Hand Side branch doesn't exist"))
            currentNode = decisionTree.getOrElse(temp, throw new RuntimeException(
              "Right Hand Side branch doesn't exist"))
            if (currentNode.children != None) {
              tempChildrenList = currentNode.children.get
            }
            leaf = currentNode.nodeId
          }
        }
        case Some(AttributeType.Nominal) => {
          val temp = tempChildrenList.getOrElse(dataPointFeatures(currentNode.splitAttribute.get),
            throw new RuntimeException(s"--dataPointFeatures: $dataPointFeatures------:\n" +
              s"and ${dataPointFeatures(currentNode.splitAttribute.get)} \n" +
              s"-$tempChildrenList--- " +
              s"currentNode split attribute: ${currentNode.splitAttribute}!!"))

          currentNode = decisionTree.getOrElse(temp, throw new RuntimeException
          ("I ve got the powerrrrrrrr!!---------------------------1"))
          if (currentNode.children != None) {
            tempChildrenList = currentNode.children.get
          }
          leaf = currentNode.nodeId

        }
      }
    }
    leaf
  }

  /** Grows a tree, meaning that it will split a leaf with the given attribute
    * that gives the maximum information gain
    *
    * @param leafToSplit The id of the leaf to be split
    * @param splitAttribute The id of the split attribute of the node
    * @param attrType The type of the attribute: either  [[AttributeType.Nominal]] or
    *                 [[AttributeType.Numerical]]
    * @param splitValue The Value of the splitting. Applies for numerical attributes
    * @param infoGain The information gain of this splitting
    */
  def growTree(leafToSplit: Int, splitAttribute: Int, attrType: AttributeType,
    splitValue: List[Double], infoGain: Double): Unit = {
    val nodeToSplit = decisionTree.getOrElse(leafToSplit, throw new RuntimeException("There is " +
      " no leaf to split with that Id"))
    if (nodeToSplit.isLeaf) {
      val attributesToExclude = nodeToSplit.getExcludingAttributes match {
        case None =>
          mutable.Seq[Int](splitAttribute)
        case _ =>
          nodeToSplit.getExcludingAttributes.get :+ (splitAttribute)
      }
      //      System.err.println(s"node: $nodeToSplit, excludingAttr:$attributesToExclude")
      val newNodes = nodeToSplit.splitNode(splitAttribute, attrType, splitValue, infoGain,
        attributesToExclude, decisionTree.size - 1)
      nodeToSplit.isLeaf = false
      decisionTree = decisionTree ++ newNodes
    }

  }

  def getNodeExcludingAttributes(node: Int): Option[mutable.Seq[Int]] = {
    decisionTree.getOrElse(node, None) match {
      case treeNode: DTNode =>
        treeNode.getExcludingAttributes
      case None =>
        None
    }
  }

  def setNodeLabel(node: Int, label: Double): Unit = {
    decisionTree.get(node).get match {
      case node: DTNode =>
        node.setLabel(label)

    }
  }

  def getNodeLabel(node: Int): Double = {
    var label = Double.NaN
    decisionTree.get(node).get match {
      case node: DTNode =>
        label = node.getLabel
    }
    label
  }

  def nodeIsLeaf(node: Int): Boolean = {
    decisionTree.apply(node).isLeaf
  }

  def getDecisionTreeSize: Int = {
    decisionTree.size
  }

  override def toString(): String = {
    s"DecisionTree:$decisionTree"
  }

}

/**
 * @param isRoot: True only for the root of the tree
 * @param isLeaf: True if a node is a Leaf
 * @param nodeId: A unique integer, identifying each one of the tree's nodes
 *
 */
case class DTNode(
  isRoot: Boolean,
  var isLeaf: Boolean,
  nodeId: Int,
  parent: Option[Int])
  extends Serializable {

  /**
   * [[children]]: List of DecisionTreeModel branches of a Node of a Decision Tree. In case of
   * a continuous attribute, only two elements exist in the HashMap:
   * children(0) -> is the left hand side of the tree, for
   * attributeValue <= [[attributeSplitValue]].
   *
   * children(1) -> is the right hand side of the tree, for
   * attributeValue > [[attributeSplitValue]].
   *
   * [[splitAttribute]]: The Id of the splitting attribute
   * [[splitAttributeType]]: Two possible values: [[AttributeType.Nominal]] and
   * [[AttributeType.Numerical]]
   * [[attributeSplitValue]]: In case the splitting attribute is a Nominal attribute, this is
   * equal to NaN
   * [[informationGain]]: The information Gain that obtain by the selected splitting attribute
   * [[label]]: The Class that receives either majority vote or maximum probability in the
   * specific leaf
   *
   */
  var children: Option[mutable.Map[Double, Int]] = None
  var splitAttribute: Option[Int] = None
  var splitAttributeType: Option[AttributeType] = None
  var attributeSplitValue: Option[List[Double]] = None
  var informationGain = Double.NaN
  var label = Double.NaN
  var excludingAttributes: Option[mutable.Seq[Int]] = None

  /** The given node is split in two or more branches, by the use of  a Numerical
    * or Nominal attribute
    *
    * @param splitAttr The id of the split attribute of the node
    * @param splitAttrType The type of the attribute: either  [[AttributeType.Nominal]] or
    *                      [[AttributeType.Numerical]]
    * @param attrSplitValues The Value of the splitting. Applies for numerical attributes
    * @param infoGain The information gain of this splitting
    */
  def splitNode(splitAttr: Int, splitAttrType: AttributeType, attrSplitValues: List[Double],
    infoGain: Double, excludeAttributes: mutable.Seq[Int], size: Int): mutable
  .Map[Int, DTNode] = {

    val tempNodes = mutable.HashMap[Int, DTNode]()
    val tempChildren = mutable.HashMap[Double, Int]()


    splitAttribute = Some(splitAttr)
    splitAttributeType = Some(splitAttrType)
    attributeSplitValue = Some(attrSplitValues)
    informationGain = infoGain

    //      println(s"--------node:$nodeId, isLeaf:$isLeaf, splitAttrType:$splitAttrType, " +
    //        s"attrSplitValues:$attrSplitValues")

    if (splitAttributeType.get == AttributeType.Numerical) {
      var temp = DTNode(false, true, size + 1, Some(nodeId))
      //temp.excludingAttributes = Some(excludeAttributes)
      tempNodes.put(size + 1, temp)
      tempChildren.put(0.0, size + 1)

      temp = DTNode(false, true, size + 2, Some(nodeId))
      //temp.excludingAttributes = Some(excludeAttributes)
      tempNodes.put(size + 2, temp)
      tempChildren.put(1.0, size + 2)
    }
    else {
      for (i <- 0 until attrSplitValues.size) {
        val temp = DTNode(false, true, size + i + 1, Some(nodeId))
        //temp.excludingAttributes = Some(excludeAttributes)
        tempNodes.put(size + i + 1, temp)
        tempChildren.put(attrSplitValues(i), size + i + 1)
      }
    }
    children = Some(tempChildren)
    tempNodes
  }

  def getLabel: Double = {
    this.label
  }

  def setLabel(nodeLabel: Double): Unit = {
    this.label = nodeLabel
  }

  def getExcludingAttributes: Option[mutable.Seq[Int]] = {
    excludingAttributes
  }

  override def toString(): String = {
    val s = new StringBuilder()
    s.append(s"NodeId:$nodeId -> children:$children, splitting attribute: $splitAttribute, " +
      s"splitting value:$attributeSplitValue, parent:$parent, excluding " +
      s"attributes:$excludingAttributes")
    s.toString()
  }
}

