/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.util;

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.OperationRouting;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.HashAlgorithm;
import org.havenask.test.HavenaskTestCase;

import java.util.List;

import static org.havenask.engine.util.RangeUtil.calculateScaledShardId;

public class RangeUtilTests extends HavenaskTestCase {

    private void checkSplitRange(int rangeFrom, int rangeTo, int partitionCount, String expectRangeStr) {
        List<RangeUtil.PartitionRange> rangeVec = RangeUtil.splitRange(rangeFrom, rangeTo, partitionCount);
        StringBuilder rangeStr = new StringBuilder();
        for (int i = 0; i < rangeVec.size(); ++i) {
            if (i > 0) {
                rangeStr.append(" ");
            }
            rangeStr.append(rangeVec.get(i).first).append("_").append(rangeVec.get(i).second);
        }
        assertEquals(expectRangeStr, rangeStr.toString());
    }

    private void testGetRangeIdxByHashId(int rangeFrom, int rangeTo, int partCnt) {
        List<RangeUtil.PartitionRange> rangeVec = RangeUtil.splitRange(rangeFrom, rangeTo, partCnt);
        for (int i = 0; i < rangeVec.size(); ++i) {
            for (int j = rangeVec.get(i).first; j <= rangeVec.get(i).second; ++j) {
                assertEquals(i, RangeUtil.getRangeIdxByHashId(rangeFrom, rangeTo, partCnt, j));
            }
        }
    }

    public void testGetRange() {
        {
            assertNull(RangeUtil.getRange(1, 2));
            assertNull(RangeUtil.getRange(0, 0));
            assertNull(RangeUtil.getRange(65537, 0));
        }
        {
            RangeUtil.PartitionRange range = RangeUtil.getRange(1, 0);
            assertEquals(new RangeUtil.PartitionRange(0, 65535), range);
        }
        {
            RangeUtil.PartitionRange range = RangeUtil.getRange(256, 0);
            assertEquals(new RangeUtil.PartitionRange(0, 255), range);
            range = RangeUtil.getRange(256, 255);
            assertEquals(new RangeUtil.PartitionRange(65280, 65535), range);
        }
        {
            RangeUtil.PartitionRange range = RangeUtil.getRange(65536, 0);
            assertEquals(new RangeUtil.PartitionRange(0, 0), range);
            range = RangeUtil.getRange(65536, 65535);
            assertEquals(new RangeUtil.PartitionRange(65535, 65535), range);
        }
    }

    public void testSplitRange() {
        checkSplitRange(0, 999, 2, "0_499 500_999");
        checkSplitRange(0, 999, 3, "0_333 334_666 667_999");
        checkSplitRange(0, 0, 2, "0_0");
        checkSplitRange(0, 0, 1, "0_0");
        checkSplitRange(0, 1, 1, "0_1");
        checkSplitRange(0, 3, 0, "");
        checkSplitRange(0, 4, 3, "0_1 2_3 4_4");
        checkSplitRange(1, 3, 4, "1_1 2_2 3_3");
        checkSplitRange(1, 3, 3, "1_1 2_2 3_3");
        checkSplitRange(100, 199, 3, "100_133 134_166 167_199");
        checkSplitRange(1, 20, 3, "1_7 8_14 15_20");
        checkSplitRange(35533, 65535, 3, "35533_45533 45534_55534 55535_65535");
        checkSplitRange(0, 65535, 0, "");
        checkSplitRange(0, 65535, 1, "0_65535");
        checkSplitRange(
            0,
            65535,
            256,
            "0_255 256_511 512_767 768_1023 1024_1279 1280_1535 1536_1791 1792_2047 2048_2303 2304_2559 2560_2815 "
                + "2816_3071 3072_3327 3328_3583 3584_3839 3840_4095 4096_4351 4352_4607 4608_4863 4864_5119 5120_5375 5376_5631 "
                + "5632_5887 5888_6143 6144_6399 6400_6655 6656_6911 6912_7167 7168_7423 7424_7679 7680_7935 7936_8191 8192_8447 "
                + "8448_8703 8704_8959 8960_9215 9216_9471 9472_9727 9728_9983 9984_10239 10240_10495 10496_10751 10752_11007 "
                + "11008_11263 11264_11519 11520_11775 11776_12031 12032_12287 12288_12543 12544_12799 12800_13055 13056_13311 "
                + "13312_13567 13568_13823 13824_14079 14080_14335 14336_14591 14592_14847 14848_15103 15104_15359 15360_15615 "
                + "15616_15871 15872_16127 16128_16383 16384_16639 16640_16895 16896_17151 17152_17407 17408_17663 17664_17919 "
                + "17920_18175 18176_18431 18432_18687 18688_18943 18944_19199 19200_19455 19456_19711 19712_19967 19968_20223 "
                + "20224_20479 20480_20735 20736_20991 20992_21247 21248_21503 21504_21759 21760_22015 22016_22271 22272_22527 "
                + "22528_22783 22784_23039 23040_23295 23296_23551 23552_23807 23808_24063 24064_24319 24320_24575 24576_24831 "
                + "24832_25087 25088_25343 25344_25599 25600_25855 25856_26111 26112_26367 26368_26623 26624_26879 26880_27135 "
                + "27136_27391 27392_27647 27648_27903 27904_28159 28160_28415 28416_28671 28672_28927 28928_29183 29184_29439 "
                + "29440_29695 29696_29951 29952_30207 30208_30463 30464_30719 30720_30975 30976_31231 31232_31487 31488_31743 "
                + "31744_31999 32000_32255 32256_32511 32512_32767 32768_33023 33024_33279 33280_33535 33536_33791 33792_34047 "
                + "34048_34303 34304_34559 34560_34815 34816_35071 35072_35327 35328_35583 35584_35839 35840_36095 36096_36351 "
                + "36352_36607 36608_36863 36864_37119 37120_37375 37376_37631 37632_37887 37888_38143 38144_38399 38400_38655 "
                + "38656_38911 38912_39167 39168_39423 39424_39679 39680_39935 39936_40191 40192_40447 40448_40703 40704_40959 "
                + "40960_41215 41216_41471 41472_41727 41728_41983 41984_42239 42240_42495 42496_42751 42752_43007 43008_43263 "
                + "43264_43519 43520_43775 43776_44031 44032_44287 44288_44543 44544_44799 44800_45055 45056_45311 45312_45567 "
                + "45568_45823 45824_46079 46080_46335 46336_46591 46592_46847 46848_47103 47104_47359 47360_47615 47616_47871 "
                + "47872_48127 48128_48383 48384_48639 48640_48895 48896_49151 49152_49407 49408_49663 49664_49919 49920_50175 "
                + "50176_50431 50432_50687 50688_50943 50944_51199 51200_51455 51456_51711 51712_51967 51968_52223 52224_52479 "
                + "52480_52735 52736_52991 52992_53247 53248_53503 53504_53759 53760_54015 54016_54271 54272_54527 54528_54783 "
                + "54784_55039 55040_55295 55296_55551 55552_55807 55808_56063 56064_56319 56320_56575 56576_56831 56832_57087 "
                + "57088_57343 57344_57599 57600_57855 57856_58111 58112_58367 58368_58623 58624_58879 58880_59135 59136_59391 "
                + "59392_59647 59648_59903 59904_60159 60160_60415 60416_60671 60672_60927 60928_61183 61184_61439 61440_61695 "
                + "61696_61951 61952_62207 62208_62463 62464_62719 62720_62975 62976_63231 63232_63487 63488_63743 63744_63999 "
                + "64000_64255 64256_64511 64512_64767 64768_65023 65024_65279 65280_65535"
        );
    }

    public void testGetRangeIdx() {
        assertEquals(-1, RangeUtil.getRangeIdx(65535, 0, 1, new RangeUtil.PartitionRange(0, 65535)));
        assertEquals(-1, RangeUtil.getRangeIdx(0, 65535, 0, new RangeUtil.PartitionRange(0, 65535)));
        assertEquals(0, RangeUtil.getRangeIdx(0, 65535, 1, new RangeUtil.PartitionRange(0, 65535)));
        assertEquals(2, RangeUtil.getRangeIdx(0, 65535, 256, new RangeUtil.PartitionRange(512, 767)));
        assertEquals(-1, RangeUtil.getRangeIdx(0, 65535, 256, new RangeUtil.PartitionRange(513, 767)));
    }

    public void testGetRangeIdxByHashId() {
        testGetRangeIdxByHashId(0, RangeUtil.MAX_PARTITION_RANGE, RangeUtil.MAX_PARTITION_RANGE);
        testGetRangeIdxByHashId(RangeUtil.MAX_PARTITION_RANGE / 2, RangeUtil.MAX_PARTITION_RANGE, 100);
        testGetRangeIdxByHashId(RangeUtil.MAX_PARTITION_RANGE, RangeUtil.MAX_PARTITION_RANGE, 1);
        for (int i = 1; i <= 1024; ++i) {
            testGetRangeIdxByHashId(0, RangeUtil.MAX_PARTITION_RANGE, i);
        }
    }

    // test calculateScaledShardId
    public void testCalculateScaledShardId() {

        // default
        {
            IndexMetadata indexMetadata = IndexMetadata.builder("test")
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 10)
                        .put("index.number_of_replicas", 0)
                )
                .build();
            assertEquals(
                OperationRouting.defaultCalculateScaledShardId(indexMetadata, "0", 0),
                calculateScaledShardId(indexMetadata, "0", 0)
            );
            assertEquals(
                OperationRouting.defaultCalculateScaledShardId(indexMetadata, "1", 1),
                calculateScaledShardId(indexMetadata, "1", 1)
            );
            assertEquals(
                OperationRouting.defaultCalculateScaledShardId(indexMetadata, "2", 0),
                calculateScaledShardId(indexMetadata, "2", 0)
            );
            assertEquals(
                OperationRouting.defaultCalculateScaledShardId(indexMetadata, "3", 1),
                calculateScaledShardId(indexMetadata, "3", 1)
            );
        }

        // havenask
        {
            IndexMetadata indexMetadata = IndexMetadata.builder("test")
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 10)
                        .put("index.number_of_replicas", 0)
                        .put("index.engine", "havenask")
                )
                .build();
            assertEquals(
                RangeUtil.getRangeIdxByHashId(
                    0,
                    HashAlgorithm.HASH_SIZE - 1,
                    indexMetadata.getNumberOfShards(),
                    (int) HashAlgorithm.getHashId("0")
                ),
                calculateScaledShardId(indexMetadata, "0", 0)
            );
            assertEquals(
                RangeUtil.getRangeIdxByHashId(
                    0,
                    HashAlgorithm.HASH_SIZE - 1,
                    indexMetadata.getNumberOfShards(),
                    (int) HashAlgorithm.getHashId("1")
                ),
                calculateScaledShardId(indexMetadata, "1", 1)
            );
            assertEquals(
                RangeUtil.getRangeIdxByHashId(
                    0,
                    HashAlgorithm.HASH_SIZE - 1,
                    indexMetadata.getNumberOfShards(),
                    (int) HashAlgorithm.getHashId("2")
                ),
                calculateScaledShardId(indexMetadata, "2", 0)
            );
        }
    }
}
