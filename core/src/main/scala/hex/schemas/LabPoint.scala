/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hex.schemas

class LabPoint (val Label      :Option[Int],
                val Vector0    :Option[Double],
                val Vector1    :Option[Double],
                val Vector2    :Option[Double],
                val Vector3    :Option[Double],
                val Vector4    :Option[Double]
) extends Product with Serializable {

  override def canEqual(that: Any):Boolean = that.isInstanceOf[LabPoint]
  override def productArity: Int = 6
  override def productElement(n: Int): Option[Any] = n match {
    case  0 => Label
    case  1 => Vector0
    case  2 => Vector1
    case  3 => Vector2
    case  4 => Vector3
    case  5 => Vector4
    case  _ => throw new IndexOutOfBoundsException(n.toString)
  }
  override def toString:String = {
    val sb = new StringBuffer
    for( i <- 0 until productArity )
      sb.append(productElement(i)).append(',')
    sb.toString
  }

  def isWrongRow():Boolean = (0 until productArity).map( idx => productElement(idx)).forall(e => e==None)
}
