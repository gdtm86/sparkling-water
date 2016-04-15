package org.apache.spark.ml.param.h2o

import org.apache.spark.ml.param._
import water.{Keyed, Key}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by kuba on 18/04/16.
  */
class H2OKeyParam[T<:Keyed[T]](parent: Params, name: String, doc: String, isValid: Key[T] => Boolean)
  extends Param[Key[T]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Key[T]): ParamPair[Key[T]] = super.w(value)

  override def jsonEncode(value: Key[T]): String = {
    compact(render(H2OKeyParam.jValueEncode[T](value)))
  }

  override def jsonDecode(json: String): Key[T] = {
    H2OKeyParam.jValueDecode[T](parse(json))
  }
}

private object H2OKeyParam{

  /** Encodes a param value into JValue. */
  def jValueEncode[T<:Keyed[T]](value: Key[T]): JValue = {
    if (value == null){
      JNull
    }else{
      JString(value.toString)
    }
  }

  /** Decodes a param value from JValue. */
  def jValueDecode[T<:Keyed[T]](jValue: JValue): Key[T]= {
    jValue match {
      case JString(x) =>
        Key.make[T](x)
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jValue to Key[T].")
    }

  }
}