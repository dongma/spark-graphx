package org.apache.spark.common

import java.sql.Date

/**
 *
 * @author Sam Ma
 * @date 2024/06/08
 */
case class Person(
                   id: Int,
                   firstName: String,
                   middleName: String,
                   lastName: String,
                   gender: String,
                   birthDate: Date,
                   ssn: String,
                   salary: Int
                 )
