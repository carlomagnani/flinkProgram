/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.security.auth.login.Configuration;
import java.io.File;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class VehicleTelematics {

    public static void main(String[] args) throws Exception {
        File file = new File("src/main/resources/sample-traffic-input.txt");
        String absolutePath = file.getAbsolutePath();


        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //read the data from the file
        DataStream<String> vehiclesData = env.readTextFile(absolutePath);


        //map individual telemtery events to objects
        DataStream<TelmeteryDataPoint> events = vehiclesData
                .map(new MapFunction<String, TelmeteryDataPoint>() {
                    @Override
                    public TelmeteryDataPoint map(String line) throws Exception {
                        return TelmeteryDataPoint.fromString(line);
                    }
                });

        SpeedRadar(events).print();


        //events.print();

        env.execute("Vehicle Telematics execution");
    }


    /**
     * Detects cars that overcome the speed limit of 90 mph.
     * */
    static DataStream<TelmeteryDataPoint> SpeedRadar(DataStream<TelmeteryDataPoint> events) {
        return events.filter(new FilterFunction<TelmeteryDataPoint>() {
            @Override
            public boolean filter(TelmeteryDataPoint telmeteryDataPoint) throws Exception {
                return telmeteryDataPoint.speed > 90;
            }
        });


    }

    /**
     * Detects stopped vehicles on any segment. A vehicle is stopped when it reports at
     * least 4 consecutive events from the same position.
     * */
    static DataStream<TelmeteryDataPoint> AccidentReporter(DataStream<TelmeteryDataPoint> events) {
        return events.filter(new FilterFunction<TelmeteryDataPoint>() {
            @Override
            public boolean filter(TelmeteryDataPoint telmeteryDataPoint) throws Exception {
                return telmeteryDataPoint.speed > 90;
            }
        });


    }

    /**
     * detects cars with an average speed higher than 60 mph between segments 52 and 56 (both included)
     * in both directions. If a car sends several reports on segments 52 or 56,
     * the ones taken for the average speed are the ones that cover a longer distance..
     * */
    static DataStream<TelmeteryDataPoint> AverageSpeedControl(DataStream<TelmeteryDataPoint> events) {
        return events.filter(new FilterFunction<TelmeteryDataPoint>() {
            @Override
            public boolean filter(TelmeteryDataPoint telmeteryDataPoint) throws Exception {
                return telmeteryDataPoint.speed > 90;
            }
        });

    }
}
