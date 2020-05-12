/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.*;

import static org.elasticsearch.rest.action.admin.cluster.RestNodesInfoAction.ALLOWED_METRICS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesInfoActionTests extends ESTestCase {
    private final static Set<String> ALLOWED_METRICS = Sets.newHashSet("http", "jvm", "os", "plugins", "process", "settings", "thread_pool", "transport");
    private SettingsFilter settingsFilter;

    public void testDuplicatedFiltersAreNotRemoved() {
        Map<String, String> params = new HashMap<>();
        params.put("nodeId", "_all,master:false,_all");

        RestRequest restRequest = buildRestRequest(params);
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { "_all", "master:false", "_all" }, actual.nodesIds());
    }


    public void RestNodesInfoAction(Settings settings, RestController controller, Client client, SettingsFilter settingsFilter) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_nodes", this);
        // this endpoint is used for metrics, not for nodeIds, like /_nodes/fs
        controller.registerHandler(GET, "/_nodes/{nodeId}", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/{metrics}", this);
        // added this endpoint to be aligned with stats
        controller.registerHandler(GET, "/_nodes/{nodeId}/info/{metrics}", this);

        this.settingsFilter = settingsFilter;
    }

    public void testOnlyMetrics() {
        Map<String, String> params = new HashMap<>();
        int metricsCount = randomIntBetween(1, ALLOWED_METRICS.size());
        List<String> metrics = new ArrayList<>();

        for(int i = 0; i < metricsCount; i++) {
            metrics.add(randomFrom(ALLOWED_METRICS));
        }
        params.put("nodeId", String.join(",", metrics));

        RestRequest restRequest = buildRestRequest(params);
        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { "_all" }, actual.nodesIds());
        assertMetrics(metrics, actual);
    }

    public void testAllMetricsSelectedWhenNodeAndMetricSpecified() {
        Map<String, String> params = new HashMap<>();
        String nodeId = randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23));
        String metric = randomFrom(ALLOWED_METRICS);

        params.put("nodeId", nodeId + "," + metric);
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(new String[] { nodeId, metric }, actual.nodesIds());
        assertAllMetricsTrue(actual);
    }

    public void testSeparateNodeIdsAndMetrics() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);
        List<String> metrics = new ArrayList<>(5);

        for(int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
            metrics.add(randomFrom(ALLOWED_METRICS));
        }

        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", String.join(",", metrics));
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertMetrics(metrics, actual);
    }

    public void testExplicitAllMetrics() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);

        for(int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
        }

        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", "_all");
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertAllMetricsTrue(actual);
    }

    /**
     * Test that if a user requests a non-existent metric, it's dropped from the
     * request without an error.
     */
    public void testNonexistentMetricsDropped() {
        Map<String, String> params = new HashMap<>();
        List<String> nodeIds = new ArrayList<>(5);
        List<String> metrics = new ArrayList<>(5);

        for(int i = 0; i < 5; i++) {
            nodeIds.add(randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(23)));
            metrics.add(randomFrom(ALLOWED_METRICS));
        }
        String nonAllowedMetric = randomValueOtherThanMany(ALLOWED_METRICS::contains, () -> randomAlphaOfLength(5));
        metrics.add(nonAllowedMetric);

        params.put("nodeId", String.join(",", nodeIds));
        params.put("metrics", String.join(",", metrics));
        RestRequest restRequest = buildRestRequest(params);

        NodesInfoRequest actual = RestNodesInfoAction.prepareRequest(restRequest);
        assertArrayEquals(nodeIds.toArray(), actual.nodesIds());
        assertThat(actual.requestedMetrics(), not(hasItem(nonAllowedMetric)));
        assertMetrics(metrics, actual);
    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.GET)
                .withPath("/_nodes")
                .withParams(params)
                .build();
    }

    private void assertMetrics(List<String> metrics, NodesInfoRequest nodesInfoRequest) {
        Set<String> validRequestedMetrics = Sets.intersection(new HashSet<>(metrics), ALLOWED_METRICS);
        assertThat(nodesInfoRequest.requestedMetrics(), equalTo(validRequestedMetrics));
    }

    private void assertAllMetricsTrue(NodesInfoRequest nodesInfoRequest) {
        assertThat(nodesInfoRequest.requestedMetrics(), equalTo(ALLOWED_METRICS));
    }

    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest();
        clusterStateRequest.indicesOptions(IndicesOptions.fromRequest(request, clusterStateRequest.indicesOptions()));
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        final String[] indices = Strings.splitStringByCommaToArray(request.param("indices", "_all"));
        boolean isAllIndicesOnly = indices.length == 1 && "_all".equals(indices[0]);
        if (!isAllIndicesOnly) {
            clusterStateRequest.indices(indices);
        }

        if (request.hasParam("metric")) {
            EnumSet<ClusterState.Metric> metrics = ClusterState.Metric.parseString(request.param("metric"), true);
            // do not ask for what we do not need.
            clusterStateRequest.nodes(metrics.contains(ClusterState.Metric.NODES) || metrics.contains(ClusterState.Metric.MASTER_NODE));
            //there is no distinction in Java api between routing_table and routing_nodes, it's the same info set over the wire, one single flag to ask for it
            clusterStateRequest.routingTable(metrics.contains(ClusterState.Metric.ROUTING_TABLE) || metrics.contains(ClusterState.Metric.ROUTING_NODES));
            //clusterStateRequest.metaData(metrics.contains(ClusterState.Metric.METADATA));
            clusterStateRequest.blocks(metrics.contains(ClusterState.Metric.BLOCKS));
            clusterStateRequest.customs(metrics.contains(ClusterState.Metric.CUSTOMS));
        }
        settingsFilter.addFilterSettingParams(request);

        client.admin().cluster().state(clusterStateRequest, new RestBuilderListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(RestClusterStateAction.Fields.CLUSTER_NAME, response.getClusterName().value());
                response.getState().toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

}
