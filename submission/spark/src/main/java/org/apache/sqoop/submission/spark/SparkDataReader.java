/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.submission.spark;
import java.util.Iterator;
import java.util.List;

import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.etl.io.DataReader;

public class SparkDataReader extends DataReader {

    private Iterator<IntermediateDataFormat<?>> dataIterator = null;

    public SparkDataReader(List<IntermediateDataFormat<?>> data) {
        this.dataIterator = (data).iterator();
    }

    @Override
    public Object[] readArrayRecord() throws InterruptedException {
        if (dataIterator.hasNext()) {
            IntermediateDataFormat<?> element = dataIterator.next();
            return element.getObjectData();
        }
        return null;
    }

    @Override
    public String readTextRecord() throws InterruptedException {
        if (dataIterator.hasNext()) {
            IntermediateDataFormat<?> element = dataIterator.next();
            return element.getCSVTextData();
        }
        return null;
    }

    @Override
    public Object readContent() throws InterruptedException {
        if (dataIterator.hasNext()) {
            IntermediateDataFormat<?> element = dataIterator.next();
            return element.getData();
        }
        return null;
    }

}
