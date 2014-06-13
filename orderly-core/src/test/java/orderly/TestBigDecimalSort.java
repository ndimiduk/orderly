package orderly;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestBigDecimalSort {

    @Test
    public void sort_positiveValue_negativeExponents() throws IOException {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("0"),
                new BigDecimal(".5"),
                new BigDecimal(".55"),
                new BigDecimal(".555"),
                new BigDecimal(".5555"),
                new BigDecimal(".55555"),
                new BigDecimal(".555555"),
                new BigDecimal(".5555555"),
                new BigDecimal(".55555555"),
                new BigDecimal(".555555555"),
                new BigDecimal(".5555555555"),
                new BigDecimal(".55555555555"),
                new BigDecimal(".55555555555444444444444444444444444"),
                new BigDecimal(".555555555555"),
                new BigDecimal(".5555555555555"),
                new BigDecimal(".55555555555555"),
                new BigDecimal(".555555555555555"),
                new BigDecimal(".5555555555555555"),
                new BigDecimal(".5555555555555555555555555555555555")
        );
        assertSerializationAndOrder(bigDecimalList, Order.ASCENDING);
        assertSerializationAndOrder(bigDecimalList, Order.DESCENDING);
    }

    @Test
    public void sort_positiveValue_positiveExponents() throws IOException {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("0"),
                new BigDecimal("5"),
                new BigDecimal("55"),
                new BigDecimal("555"),
                new BigDecimal("5555"),
                new BigDecimal("55555"),
                new BigDecimal("555555"),
                new BigDecimal("5555555"),
                new BigDecimal("55555555"),
                new BigDecimal("555555555"),
                new BigDecimal("5555555555"),
                new BigDecimal("55555555555"),
                new BigDecimal("555555555555"),
                new BigDecimal("5555555555555"),
                new BigDecimal("55555555555555"),
                new BigDecimal("555555555555555"),
                new BigDecimal("5555555555555555"),
                new BigDecimal("5555555555555555555555555555555555")
        );
        assertSerializationAndOrder(bigDecimalList, Order.ASCENDING);
        assertSerializationAndOrder(bigDecimalList, Order.DESCENDING);
    }

    @Test
    public void sort_negativeValue_negativeExponents() throws IOException {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-.5555555555555555555555555555555555"),
                new BigDecimal("-.5555555555555555"),
                new BigDecimal("-.555555555555555"),
                new BigDecimal("-.55555555555555"),
                new BigDecimal("-.5555555555555"),
                new BigDecimal("-.555555555555"),
                new BigDecimal("-.55555555555"),
                new BigDecimal("-.5555555555"),
                new BigDecimal("-.555555555"),
                new BigDecimal("-.55555555"),
                new BigDecimal("-.5555555"),
                new BigDecimal("-.555555"),
                new BigDecimal("-.55555"),
                new BigDecimal("-.5555"),
                new BigDecimal("-.555"),
                new BigDecimal("-.55"),
                new BigDecimal("-.5"),
                new BigDecimal("0")
        );
        assertSerializationAndOrder(bigDecimalList, Order.ASCENDING);
        assertSerializationAndOrder(bigDecimalList, Order.DESCENDING);
    }

    @Test
    public void sort_negativeValue_positiveExponents() throws IOException {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-5555555555555555555555555555555555"),
                new BigDecimal("-5555555555555555"),
                new BigDecimal("-555555555555555"),
                new BigDecimal("-55555555555555"),
                new BigDecimal("-5555555555555"),
                new BigDecimal("-555555555555"),
                new BigDecimal("-55555555555"),
                new BigDecimal("-5555555555"),
                new BigDecimal("-555555555"),
                new BigDecimal("-55555555"),
                new BigDecimal("-5555555"),
                new BigDecimal("-555555"),
                new BigDecimal("-55555"),
                new BigDecimal("-5555"),
                new BigDecimal("-555"),
                new BigDecimal("-55"),
                new BigDecimal("-5"),
                new BigDecimal("0")
        );
        assertSerializationAndOrder(bigDecimalList, Order.ASCENDING);
        assertSerializationAndOrder(bigDecimalList, Order.DESCENDING);
    }

    @Test
    public void sort_positiveValues_positiveExponents_AND_negativeExponents() throws IOException {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("0"),
                new BigDecimal(".5"),
                new BigDecimal(".55"),
                new BigDecimal(".555"),
                new BigDecimal(".5555"),
                new BigDecimal(".55555"),
                new BigDecimal(".555555"),
                new BigDecimal(".5555555"),
                new BigDecimal(".55555555"),
                new BigDecimal(".555555555"),
                new BigDecimal("5"),
                new BigDecimal("55"),
                new BigDecimal("555"),
                new BigDecimal("5555"),
                new BigDecimal("55555"),
                new BigDecimal("555555"),
                new BigDecimal("5555555"),
                new BigDecimal("55555555")
        );
        assertSerializationAndOrder(bigDecimalList, Order.ASCENDING);
        assertSerializationAndOrder(bigDecimalList, Order.DESCENDING);
    }

    @Test
    public void sort_negativeValues_positiveExponents_AND_negativeExponents() throws IOException {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-55555555"),
                new BigDecimal("-5555555"),
                new BigDecimal("-555555"),
                new BigDecimal("-55555"),
                new BigDecimal("-5555"),
                new BigDecimal("-555"),
                new BigDecimal("-55"),
                new BigDecimal("-5"),
                new BigDecimal("-.555555555"),
                new BigDecimal("-.55555555"),
                new BigDecimal("-.5555555"),
                new BigDecimal("-.555555"),
                new BigDecimal("-.55555"),
                new BigDecimal("-.5555"),
                new BigDecimal("-.555"),
                new BigDecimal("-.55"),
                new BigDecimal("-.5"),
                new BigDecimal("0")
        );
        assertSerializationAndOrder(bigDecimalList, Order.ASCENDING);
        assertSerializationAndOrder(bigDecimalList, Order.DESCENDING);
    }

    private void assertSerializationAndOrder(List<BigDecimal> bigDecimalList, Order order) throws IOException {
        List<BigDecimal> bigDecimalListCopy = new ArrayList<BigDecimal>(bigDecimalList);
        Collections.sort(bigDecimalListCopy);
        assertEquals("Test data was not in ascending order", bigDecimalListCopy, bigDecimalList);

        if (order == Order.DESCENDING) {
            Collections.reverse(bigDecimalListCopy);
        }

        doAssert(order, bigDecimalListCopy);
    }

    private void doAssert(Order order, List<BigDecimal> bigDecimalListCopy) throws IOException {
        // create serializer
        BigDecimalRowKey bigDecRowKey = new BigDecimalRowKey();
        bigDecRowKey.setOrder(order);

        // serialize to list of bytes
        List<byte[]> bytesList = serializeList(bigDecimalListCopy, bigDecRowKey);

        // shuffle, then sort bytes
        Collections.shuffle(bytesList);
        Collections.sort(bytesList, Bytes.BYTES_COMPARATOR);

        // deserialize
        List<BigDecimal> deserialized = deserializeList(bigDecRowKey, bytesList);

        assertEquals("failed, order=" + order, bigDecimalListCopy, deserialized);
    }

    private List<BigDecimal> deserializeList(BigDecimalRowKey bigDecRowKey, List<byte[]> bytesList) throws IOException {
        List<BigDecimal> de = Lists.newArrayList();
        for (byte[] a : bytesList) {
            de.add((BigDecimal) bigDecRowKey.deserialize(a));
        }
        return de;
    }

    private List<byte[]> serializeList(List<BigDecimal> bigDecimalList, BigDecimalRowKey bigDecRowKey) throws IOException {
        List<byte[]> bytesList = Lists.newArrayList();
        for (BigDecimal b : bigDecimalList) {
            bytesList.add(bigDecRowKey.serialize(b));
        }
        return bytesList;
    }

}
