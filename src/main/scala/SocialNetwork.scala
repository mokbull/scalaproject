import org.apache.spark.{SparkConf, SparkContext}


object SocialNetwork {

  // Function to parse each person's friend list and add it to the other values
  def parseFriends(row: Array[String]) : List[String] = {
    val arr = row
    val friends : List[String] = {
      if (arr.size > 2)
        List(arr(0), arr(1)) ::: arr(2).split(",").toList
      else
        List(arr(0), arr(1))
    }
    friends
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Social Network").setMaster("local")
    val sc = new SparkContext(conf)

    // Bring in the .tsvs as RDDs
    val m_src = sc.textFile("src/main/m.tsv")
    val f_src = sc.textFile("src/main/f.tsv")

    // Adds M or F before id, splits by tab, and does some other RDD operations
    val m_parsed = m_src.map(row => "M".concat(row)).map(row => row.split("\t"))
    val f_parsed = f_src.map(row => "F".concat(row)).map(row => row.split("\t"))
    val combined = f_parsed.union(m_parsed).map(parseFriends).collect().toList

    // Create map of id -> name + 1st order friends
    val id_map = collection.mutable.Map[String, List[String]]()

    // Create map of name -> list of 1st/2nd order friends
    val name_map = collection.mutable.Map[String, List[String]]()

    // Fills the maps created above
    for (id <- combined) {
      val f_id = id(0)
      val f_name = id(1)
      id_map(f_id) = id.drop(1)
      name_map(f_name) = id.drop(2)
    }

    // Adds all friends' friends to list of 1st order friends, allowing for duplicates which are removed later
    for ((k,v) <- id_map) {
      val name = v(0)
      val k_friends = {
        if (v.size > 1)
          v.drop(1)
        else
          Nil
      }
      if (k_friends != Nil) {
        for (friend <- k_friends) {
          val friends_friends = id_map(friend).drop(1)
          val mapped_friends = name_map(name)
          val all_friends = mapped_friends ::: friends_friends
          name_map(name) = all_friends
        }
      }
    }

    // Remove duplicates from concatenated lists, and sort by size of social network
    val network_distinct = name_map.mapValues(_.distinct)
    val output = network_distinct.toList
    val sorted_output = output.sortWith(_._2.size > _._2.size)

    // Convert the id's of each friend to the matching names
    val builder = StringBuilder.newBuilder
    builder ++= "TOP 3 SOCIAL NETWORKS: \n"
    for (person <- sorted_output.take(3)) {
      builder ++= person._1
      builder ++= ": "
      for (friend <- person._2) {
        val friend_name = id_map(friend)(0)
        builder ++= friend_name
        builder ++= ", "
      }
      builder ++= "\n"
    }

    println(builder.toString())
    sc.stop()
  }
}
