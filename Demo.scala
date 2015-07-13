trait ClassEnhancer {
  def productIterator: Iterator[Any]
  def toMap= (this.getClass.getDeclaredFields.map(n => n.getName).toSeq
    zip this.productIterator.toList.toSeq).toMap[String,Any]
  //def zip(values:Seq[Any])=fieldNames.zip(values)
  def fieldNames:Seq[String]=this.getClass.getDeclaredFields.toList.map(n => n.getName).toSeq

}

val objensis = new ObjenesisStd

def create[A](implicit m: scala.reflect.Manifest[A]): A =
  objensis.newInstance(m.erasure).asInstanceOf[A]

implicit def any2WithFields[A](o: A) = new AnyRef {

  def fieldNames:Seq[String]=o.getClass.getDeclaredFields.toList.map(n => n.getName).toSeq
  def fieldTypes:Seq[String]=o.getClass.getDeclaredFields.toList.map(n => n.getGenericType.toString).toSeq
  //def fieldNames:Seq[String]=this.getClass.getDeclaredFields.toList.map(n => n.getName).toSeq
  def fromValues(values: Seq[Any]): A = fromMap(fieldNames.zip(values).toMap[String,Any])
  def fromMap(values: Pair[String, Any]*): A = fromMap(Map(values :_*))
  def fromMap(values: Map[String, Any]): A = {
    for ((name, value) <- values) setField(o, name, value)
    o
  }
  def setField(o: Any, fieldName: String, fieldValue: Any) {
    o.getClass.getDeclaredFields.find(_.getName == fieldName.trim) match {
      case Some(field) => {
        val ft = field.getGenericType.toString.trim
        val fv = fieldValue.toString.trim
        field.setAccessible(true)
        field.set(o, ft match{
          case "class java.lang.String" => fv
          case "double" => fv.toDouble
          case "int" => fv.toInt
          case "boolean" => fv.toBoolean
          case _=> "NotSupported "
        })
        field.setAccessible(false)

      }
      case None =>
        throw new IllegalArgumentException("No field named " + fieldName)
    }
  }
}


case class FieldData(dateTime:String,nodeName:String,nodeType:String,
                     p1:Double,p1Status:Boolean,t1:Double,t1Status:Boolean,w1:Double,w1Status:Boolean,
                     p2:Double,p2Status:Boolean,t2:Double,t2Status:Boolean,w2:Double,w2Status:Boolean,
                     myInt: Int) extends ClassEnhancer


def getFieldData(sc:SparkContext, fileName: String): DataFrame ={
  val fieldText = sc.textFile(fileName)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  // rdd
  //val fieldData=fieldText.map(s => s.split(",")).map(s => create[FieldData].fromValues(s))
  val fieldData=fieldText.map(s => create[FieldData].fromValues(s.split(",")))
  
  

  // register
  fieldData.toDF.registerTempTable("fieldData")

  fieldData.toDF()
}
