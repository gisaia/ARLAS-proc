/*
 * Licensed to Gisaïa under one or more contributor
 * license agreements. See the NOTICE.txt file distributed with
 * this work for additional information regarding copyright
 * ownership. Gisaïa licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.arlas.data.transform.tools

import io.arlas.data.transform.ArlasTest
import io.arlas.data.transform.ArlasTestHelper._
import org.apache.spark.sql.types.StringType
import io.arlas.data.sql._
import scala.collection.immutable.ListMap

class WithGeometrySimplifierTest extends ArlasTest {

  val inputGeometryCol = "geo"
  val outputGeometryCol = "simplified_geo"
  val expectedGeometryCol = "expected_simplified_geo"

  val testDF = createDataFrameWithTypes(
    spark,
    List(
      Seq(
        "LINESTRING (3.346128 41.272916, 3.364113 41.272922, 3.382205 41.272977, 3.40016 41.272907, 3.418522 41.273336, 3.43655 41.273343, 3.45444 41.273312, 3.47243 41.273281, 3.490409 41.273286, 3.508407 41.273441, 3.526365 41.273578, 3.544312 41.273641, 3.562363 41.273669, 3.5804 41.273761, 3.598468 41.273898, 3.616606 41.274046, 3.634561 41.274132, 3.652486 41.27418, 3.670443 41.274247, 3.688534 41.274297, 3.70657 41.274284, 3.724521 41.274244, 3.742482 41.274222, 3.760448 41.274115, 3.77849 41.274208, 3.796584 41.274532, 3.814777 41.274866, 3.83286 41.275078, 3.850934 41.275519, 3.868855 41.275913, 3.886962 41.276459, 3.904952 41.276937, 3.92307 41.277278, 3.940908 41.27778, 3.958974 41.278489, 3.976897 41.27881, 3.994882 41.279046, 4.012604 41.279361, 4.030459 41.279435, 4.048214 41.279864, 4.065967 41.280025, 4.083686 41.281225, 4.101329 41.282103, 4.119263 41.282569, 4.137163 41.283086, 4.154832 41.283492, 4.172679 41.283918, 4.190502 41.284052, 4.208352 41.283962, 4.226237 41.283596, 4.244167 41.283499, 4.262129 41.283411, 4.280206 41.28329, 4.298078 41.283277, 4.316318 41.283131, 4.334077 41.282784, 4.351977 41.282498, 4.369948 41.282167, 4.387953 41.281982, 4.405786 41.281649, 4.424073 41.281643, 4.441943 41.281585, 4.459913 41.281396, 4.477706 41.281005, 4.496014 41.280881, 4.514221 41.280768, 4.532427 41.280471, 4.550789 41.280269, 4.568941 41.280127, 4.587201 41.279906, 4.605407 41.279624, 4.62376 41.279333, 4.64213 41.279036, 4.660429 41.278786, 4.678676 41.278298, 4.696744 41.278168, 4.715297 41.277923, 4.733591 41.277787, 4.752036 41.277537, 4.770467 41.27733, 4.789289 41.277248, 4.807882 41.277094, 4.826332 41.276846, 4.844957 41.276632, 4.863504 41.276569, 4.881858 41.276112, 4.899996 41.275864, 4.918261 41.275517, 4.935926 41.275312, 4.954259 41.274971, 4.972773 41.274881, 4.991069 41.274676, 5.008986 41.274578, 5.026732 41.274316, 5.045003 41.274025, 5.063087 41.273598, 5.081244 41.273147, 5.099255 41.272804, 5.117278 41.27247, 5.135326 41.272004, 5.153112 41.271571, 5.171203 41.270951, 5.1892 41.270647, 5.206757 41.270439, 5.224796 41.270522, 5.242721 41.270676, 5.260749 41.270901, 5.278978 41.270946, 5.297062 41.270692, 5.314959 41.270771, 5.332992 41.270529, 5.350984 41.270539, 5.369108 41.270469, 5.386751 41.269962, 5.404663 41.269716, 5.422659 41.269446, 5.440834 41.269329, 5.459003 41.269276, 5.47684 41.269102, 5.495007 41.268835, 5.512922 41.269128, 5.530774 41.269049, 5.548918 41.269007, 5.566782 41.269077, 5.584713 41.269015, 5.602868 41.269003, 5.620995 41.268975, 5.639209 41.268824, 5.657221 41.268833, 5.675306 41.268605, 5.693177 41.268386, 5.711712 41.26838, 5.730032 41.268216, 5.747999 41.268135, 5.76566 41.268225, 5.784077 41.268138, 5.802335 41.267705, 5.820472 41.268051, 5.838547 41.268089, 5.857064 41.267677, 5.875035 41.268058, 5.893327 41.268107, 5.911616 41.268012, 5.929638 41.268338, 5.947915 41.268375, 5.966165 41.26851, 5.984477 41.268638, 6.00252 41.268724, 6.020493 41.268822, 6.038761 41.269023, 6.056919 41.269244, 6.075336 41.269608, 6.093602 41.269779, 6.111942 41.269864, 6.130123 41.269885, 6.148413 41.269988, 6.166448 41.269836, 6.184444 41.269642, 6.202302 41.269565, 6.220287 41.269743, 6.238504 41.269947, 6.256574 41.270317, 6.274568 41.270465, 6.292651 41.270507, 6.31105 41.270306, 6.329079 41.270448, 6.34692 41.270786, 6.364976 41.270861, 6.382834 41.271129, 6.40088 41.271174, 6.418846 41.271305, 6.437028 41.271471, 6.454838 41.271213, 6.473095 41.271408, 6.491127 41.271111, 6.509084 41.27107, 6.526888 41.270989, 6.544913 41.270725, 6.562952 41.27062, 6.580727 41.270607, 6.599007 41.270763, 6.616618 41.27067, 6.634549 41.270623, 6.652485 41.270551, 6.670228 41.270604, 6.688078 41.270455, 6.70595 41.270593, 6.723743 41.270596, 6.741604 41.270533, 6.759518 41.270529, 6.777477 41.270352, 6.795276 41.270497, 6.813317 41.270669, 6.831164 41.270615, 6.849044 41.270964, 6.866742 41.270813, 6.884711 41.271032, 6.902776 41.271073, 6.920867 41.271143, 6.938666 41.271339, 6.956293 41.271462, 6.974293 41.271761, 6.992331 41.271792, 7.010007 41.271861, 7.027843 41.271881, 7.045616 41.271918, 7.063624 41.271983, 7.08147 41.272198, 7.099416 41.272437, 7.117361 41.2726, 7.135554 41.272769, 7.153411 41.272863, 7.171349 41.273194, 7.189501 41.273541, 7.207608 41.273944, 7.225677 41.274142, 7.243868 41.274534, 7.261883 41.274761, 7.279998 41.274838, 7.298131 41.275105, 7.316193 41.275545, 7.334396 41.275999, 7.352484 41.276218, 7.370484 41.27637, 7.388646 41.2769, 7.406663 41.277329, 7.424687 41.277779, 7.442796 41.278234, 7.460903 41.278563, 7.479022 41.278958, 7.497151 41.27939, 7.515289 41.279714, 7.533389 41.279783, 7.551416 41.279727, 7.569774 41.279733, 7.58792 41.279746, 7.606331 41.279604, 7.624626 41.279831, 7.642956 41.280068, 7.661573 41.280185, 7.679752 41.280518, 7.697987 41.280754, 7.716288 41.280942, 7.734527 41.281465, 7.752761 41.281673, 7.771076 41.281845, 7.789205 41.281899, 7.807476 41.281994, 7.825567 41.282178, 7.843636 41.282177, 7.861716 41.282167, 7.879855 41.282058, 7.897739 41.282041, 7.915936 41.282105, 7.934038 41.282196, 7.9523 41.282015, 7.970347 41.282036, 7.988322 41.282128, 8.006299 41.281912, 8.024289 41.281749, 8.042399 41.28166, 8.060255 41.281682, 8.077984 41.281508, 8.09575 41.281762, 8.11342 41.282051, 8.131204 41.282217, 8.148941 41.28227, 8.166735 41.282426, 8.184386 41.282568, 8.20203 41.282679, 8.219362 41.282751, 8.236725 41.28313, 8.254144 41.283543, 8.271429 41.283861, 8.288895 41.284134, 8.306629 41.284502, 8.324276 41.28476, 8.341937 41.285082, 8.359384 41.285059, 8.377243 41.285042, 8.394741 41.284943, 8.412537 41.284754, 8.43013 41.284498, 8.447992 41.284255, 8.465687 41.283953, 8.483488 41.283573, 8.501274 41.283127, 8.51915 41.282677, 8.53681 41.282194, 8.554628 41.281932, 8.572484 41.281616, 8.590287 41.281404, 8.608033 41.281192, 8.625781 41.280932, 8.643331 41.280708, 8.661236 41.280724, 8.678992 41.280545, 8.696639 41.280385, 8.714287 41.28023, 8.731909 41.280164, 8.749576 41.280157, 8.767554 41.280287, 8.78544 41.280426, 8.803053 41.280538, 8.820702 41.280716, 8.838426 41.28078, 8.855934 41.280789, 8.874089 41.280985, 8.892009 41.281168, 8.90974 41.28134, 8.927601 41.281632, 8.945478 41.2829, 8.962961 41.284091, 8.98068 41.28475, 8.998385 41.284831, 9.016494 41.284985, 9.034451 41.284888, 9.052533 41.284968, 9.070795 41.284865, 9.088986 41.285082, 9.106627 41.285751, 9.124782 41.28648, 9.142981 41.285846, 9.161175 41.285227, 9.178685 41.284806, 9.196577 41.285482, 9.21436 41.285537, 9.232116 41.285731, 9.249976 41.286472, 9.267097 41.289809, 9.281091 41.297361, 9.294599 41.305678, 9.307939 41.314182, 9.32118 41.322877, 9.334668 41.331403, 9.348111 41.339995, 9.36164 41.348576, 9.375182 41.357385, 9.388652 41.365874, 9.402251 41.374369, 9.415954 41.382862, 9.429599 41.391487, 9.443069 41.40013, 9.456659 41.408746, 9.471134 41.416242, 9.4876 41.42143, 9.503926 41.426558, 9.520272 41.431543, 9.536729 41.436844, 9.553099 41.441928, 9.569541 41.446826, 9.585993 41.452217, 9.602517 41.457488, 9.619087 41.462417, 9.635639 41.467247, 9.652156 41.472448, 9.66875 41.47751, 9.685303 41.482641, 9.701729 41.487627, 9.718539 41.493018, 9.73541 41.498074, 9.752139 41.50275, 9.768898 41.507336, 9.785659 41.512016, 9.802321 41.516601, 9.819268 41.521361, 9.836033 41.526293, 9.852774 41.530976, 9.869505 41.53585, 9.886315 41.540821, 9.902916 41.545854, 9.919682 41.550897, 9.936342 41.556038, 9.952926 41.560918, 9.969794 41.566069, 9.986258 41.571062, 10.00297 41.576029, 10.019449 41.581185, 10.036107 41.585926, 10.052801 41.590785, 10.069734 41.595824, 10.086486 41.600603, 10.103316 41.60549, 10.120051 41.610379, 10.136845 41.615185, 10.153654 41.61992, 10.170436 41.624633, 10.187155 41.629587, 10.203973 41.634487, 10.220862 41.639377, 10.237578 41.644217, 10.254378 41.649125, 10.271103 41.653917, 10.287981 41.658778, 10.304757 41.663637, 10.321583 41.668385, 10.338391 41.67316, 10.354948 41.678036, 10.371662 41.682898, 10.388294 41.687611, 10.405046 41.692408, 10.421629 41.697114, 10.43832 41.701791, 10.454978 41.706601, 10.471656 41.71129, 10.488357 41.716, 10.505166 41.720653, 10.521888 41.725425, 10.538662 41.73011, 10.555217 41.735166, 10.571844 41.74006, 10.588352 41.744999, 10.604825 41.749911, 10.621311 41.754673, 10.637957 41.759546, 10.654517 41.764442, 10.670985 41.769098, 10.687492 41.773806, 10.703891 41.778655, 10.720319 41.78353, 10.73686 41.788405, 10.753477 41.793359, 10.769953 41.798244, 10.786759 41.803073, 10.803474 41.808512, 10.819843 41.813501, 10.836342 41.818321, 10.853194 41.823295, 10.869726 41.828353, 10.886159 41.833142, 10.903027 41.838207, 10.919651 41.843243, 10.93633 41.848294, 10.952948 41.853176, 10.969615 41.858331, 10.986163 41.863517, 11.002935 41.868617, 11.019546 41.873637, 11.036134 41.878757, 11.052637 41.883527, 11.069321 41.888534, 11.086072 41.893729, 11.102822 41.898687, 11.119536 41.903595, 11.136022 41.908727, 11.152542 41.913895, 11.169393 41.91909, 11.18595 41.923963, 11.202699 41.928832, 11.219188 41.93404, 11.235907 41.939063, 11.252624 41.944075, 11.26921 41.948953, 11.285857 41.953802, 11.302621 41.958968, 11.319392 41.963868, 11.336423 41.968839, 11.353033 41.974032, 11.369812 41.978816, 11.386681 41.98372, 11.403723 41.988401, 11.420546 41.993181, 11.43749 41.997813, 11.454484 42.002443, 11.471432 42.007216, 11.488449 42.011797, 11.505453 42.016554, 11.522336 42.021403, 11.539245 42.02612, 11.556115 42.030845, 11.572998 42.035467, 11.589857 42.040248, 11.606754 42.044905, 11.623761 42.049598, 11.640761 42.054209, 11.657942 42.058968, 11.675271 42.063543, 11.690485 42.070196, 11.702154 42.079244, 11.713427 42.087788, 11.724281 42.095958, 11.733302 42.102243, 11.742757 42.10575, 11.752291 42.109279, 11.759995 42.108122, 11.7669 42.105164, 11.772747 42.101834, 11.777368 42.099675, 11.779446 42.098326, 11.780456 42.09839, 11.781607 42.099582)",
        "LINESTRING (3.346128 41.272916, 3.40016 41.272907, 3.418522 41.273336, 3.490409 41.273286, 3.670443 41.274247, 3.77849 41.274208, 3.868855 41.275913, 3.958974 41.278489, 4.065967 41.280025, 4.101329 41.282103, 4.172679 41.283918, 4.190502 41.284052, 4.316318 41.283131, 4.405786 41.281649, 4.441943 41.281585, 4.587201 41.279906, 4.678676 41.278298, 4.863504 41.276569, 4.954259 41.274971, 5.045003 41.274025, 5.117278 41.27247, 5.171203 41.270951, 5.206757 41.270439, 5.278978 41.270946, 5.369108 41.270469, 5.422659 41.269446, 5.495007 41.268835, 5.512922 41.269128, 5.620995 41.268975, 5.693177 41.268386, 5.784077 41.268138, 5.802335 41.267705, 5.838547 41.268089, 5.857064 41.267677, 5.875035 41.268058, 5.911616 41.268012, 6.020493 41.268822, 6.093602 41.269779, 6.148413 41.269988, 6.202302 41.269565, 6.274568 41.270465, 6.31105 41.270306, 6.437028 41.271471, 6.562952 41.27062, 6.777477 41.270352, 6.849044 41.270964, 6.920867 41.271143, 6.974293 41.271761, 7.063624 41.271983, 7.153411 41.272863, 7.243868 41.274534, 7.298131 41.275105, 7.334396 41.275999, 7.370484 41.27637, 7.515289 41.279714, 7.606331 41.279604, 7.716288 41.280942, 7.734527 41.281465, 7.825567 41.282178, 7.988322 41.282128, 8.077984 41.281508, 8.131204 41.282217, 8.219362 41.282751, 8.341937 41.285082, 8.394741 41.284943, 8.447992 41.284255, 8.53681 41.282194, 8.643331 41.280708, 8.749576 41.280157, 8.855934 41.280789, 8.927601 41.281632, 8.962961 41.284091, 8.98068 41.28475, 9.088986 41.285082, 9.124782 41.28648, 9.178685 41.284806, 9.196577 41.285482, 9.232116 41.285731, 9.249976 41.286472, 9.267097 41.289809, 9.281091 41.297361, 9.307939 41.314182, 9.375182 41.357385, 9.456659 41.408746, 9.471134 41.416242, 9.536729 41.436844, 9.569541 41.446826, 9.602517 41.457488, 9.635639 41.467247, 9.73541 41.498074, 9.802321 41.516601, 9.852774 41.530976, 9.886315 41.540821, 10.019449 41.581185, 10.069734 41.595824, 10.170436 41.624633, 10.371662 41.682898, 10.538662 41.73011, 10.604825 41.749911, 10.687492 41.773806, 10.786759 41.803073, 10.819843 41.813501, 10.952948 41.853176, 11.036134 41.878757, 11.119536 41.903595, 11.169393 41.91909, 11.336423 41.968839, 11.353033 41.974032, 11.386681 41.98372, 11.488449 42.011797, 11.589857 42.040248, 11.675271 42.063543, 11.690485 42.070196, 11.724281 42.095958, 11.733302 42.102243, 11.752291 42.109279, 11.759995 42.108122, 11.7669 42.105164, 11.779446 42.098326, 11.780456 42.09839, 11.781607 42.099582)"
      ),
      Seq(null, null),
      Seq("", null)
    ),
    ListMap(
      inputGeometryCol -> (StringType, true),
      expectedGeometryCol -> (StringType, true)
    )
  )

  "WithGeometrySimplifier" should "simplify existing geometry" in {

    val transformedDF = testDF
      .drop(expectedGeometryCol)
      .process(
        new WithGeometrySimplifier(inputGeometryCol, outputGeometryCol)
      )

    val expectedDF = testDF.withColumnRenamed(expectedGeometryCol, outputGeometryCol)

    assertDataFrameEquality(transformedDF, expectedDF)
  }

}
