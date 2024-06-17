package org.apache.spark.common

import java.util.Date

/**
 *
 * @author Sam Ma
 * @date 2024/06/08
 */
case class Stock(
                  Company: String,
                  date: Date,
                  value: Double
                )
