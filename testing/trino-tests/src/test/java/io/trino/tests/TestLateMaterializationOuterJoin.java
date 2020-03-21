/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.LATE_MATERIALIZATION;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestLateMaterializationOuterJoin
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(LATE_MATERIALIZATION, "true")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.toString())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.toString());
        LocalQueryRunner runner = LocalQueryRunner
                .builder(sessionBuilder.build())
                .build();
        runner.createCatalog(
                "tpch",
                new TpchConnectorFactory(1), ImmutableMap.of("tpch.produce-pages", "true", "tpch.max-rows-per-page", "10"));
        return runner;
    }

    @Test(timeOut = 30_000)
    public void testOuterJoin()
    {
        assertQuery("SELECT * FROM (SELECT * FROM nation WHERE nationkey < -1) a RIGHT JOIN nation b ON a.nationkey = b.nationkey");
    }
}
