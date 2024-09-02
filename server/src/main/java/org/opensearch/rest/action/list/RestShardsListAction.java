/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Table;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.action.cat.RestTable;
import org.opensearch.rest.pagination.ShardBasedPaginationStrategy;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _list API action to output shards in pages.
 *
 * @opensearch.api
 */
public class RestShardsListAction extends AbstractListAction {

    private static final int DEFAULT_LIST_SHARDS_PAGE_SIZE_STRING = 5000;
    private static final String PAGINATED_ELEMENT_KEY = "shards";

    @Override
    public List<RestHandler.Route> routes() {
        return unmodifiableList(asList(new RestHandler.Route(GET, "/_list/shards"), new RestHandler.Route(GET, "/_list/shards/{index}")));
    }

    @Override
    public String getName() {
        return "list_shards_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_list/shards\n");
        sb.append("/_list/shards/{index}\n");
    }

    @Override
    public BaseRestHandler.RestChannelConsumer doListRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        clusterStateRequest.clear().nodes(true).routingTable(true).indices(indices).metadata(true);
        final String requestedToken = request.param("next_token");
        final int pageSize = request.paramAsInt("size", DEFAULT_LIST_SHARDS_PAGE_SIZE_STRING);
        if (pageSize < DEFAULT_LIST_SHARDS_PAGE_SIZE_STRING) {
            throw new IllegalArgumentException("size should be greater than or equal to [" + DEFAULT_LIST_SHARDS_PAGE_SIZE_STRING + "]");
        }
        final String requestedSortOrder = request.param("sort", "ascending");
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) {
                ShardBasedPaginationStrategy paginationStrategy = new ShardBasedPaginationStrategy(
                    requestedToken == null ? null : new ShardBasedPaginationStrategy.ShardStrategyPageToken(requestedToken),
                    pageSize,
                    requestedSortOrder,
                    clusterStateResponse.getState()
                );

                IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
                indicesStatsRequest.all();
                indicesStatsRequest.indices(paginationStrategy.getIndicesFromRequestedToken().toArray(new String[0]));
                client.admin().indices().stats(indicesStatsRequest, new RestResponseListener<IndicesStatsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(IndicesStatsResponse indicesStatsResponse) throws Exception {
                        return RestTable.buildResponse(
                            RestShardsAction.RestShardsActionCommonUtils.buildTable(
                                request,
                                clusterStateResponse,
                                indicesStatsResponse,
                                paginationStrategy.getElementsFromRequestedToken(),
                                new Table.PaginationMetadata(
                                    true,
                                    PAGINATED_ELEMENT_KEY,
                                    paginationStrategy.getNextToken() == null
                                        ? null
                                        : paginationStrategy.getNextToken().generateEncryptedToken()
                                )
                            ),
                            channel
                        );
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        return RestShardsAction.RestShardsActionCommonUtils.getTableWithHeader(request, null);
    }
}
