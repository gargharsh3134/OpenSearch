/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.action.admin.cluster.shards.CatShardsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.pagination.PaginatedQueryRequest;
import org.opensearch.rest.pagination.PaginatedQueryResponse;
import org.opensearch.rest.pagination.ShardBasedPaginationStrategy;

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _list API action to output shards in pages.
 *
 * @opensearch.api
 */
public class RestShardsListAction extends RestShardsAction {

    protected static final int MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE = 50000;
    protected static final int DEFAULT_LIST_SHARDS_PAGE_SIZE = 5000;
    public static final String PAGINATED_LIST_INDICES_ELEMENT_KEY = "shards";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_list/shards"), new Route(GET, "/_list/shards/{index}")));
    }

    @Override
    public String getName() {
        return "list_shards_action";
    }

    @Override
    public void documentation(StringBuilder sb) {
        sb.append("/_list/shards\n");
        sb.append("/_list/shards/{index}\n");
    }

    @Override
    public boolean isActionPaginated() {
        return true;
    }

    @Override
    protected PaginatedQueryRequest validateAndGetPaginationMetadata(RestRequest restRequest) {
        PaginatedQueryRequest paginatedQueryRequest = restRequest.parsePaginatedQueryParams("ascending", DEFAULT_LIST_SHARDS_PAGE_SIZE);
        // Validating sort order
        if (!Objects.equals(paginatedQueryRequest.getSort(), "ascending")
            && !Objects.equals(paginatedQueryRequest.getSort(), "descending")) {
            throw new IllegalArgumentException("value of sort can either be ascending or descending");
        }
        // validating pageSize
        if (paginatedQueryRequest.getSize() < DEFAULT_LIST_SHARDS_PAGE_SIZE) {
            throw new IllegalArgumentException("size must be greater than [" + DEFAULT_LIST_SHARDS_PAGE_SIZE + "]");
        } else if (paginatedQueryRequest.getSize() > MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE) {
            throw new IllegalArgumentException("size should be less than [" + MAX_SUPPORTED_LIST_SHARDS_PAGE_SIZE + "]");
        }
        // Next Token in the request will be validated by the ShardStrategyTokenParser itself.
        if (Objects.nonNull(paginatedQueryRequest.getRequestedTokenStr())) {
            ShardBasedPaginationStrategy.ShardStrategyTokenParser.validateShardStrategyToken(paginatedQueryRequest.getRequestedTokenStr());
        }

        return paginatedQueryRequest;
    }

    @Override
    public PaginatedQueryResponse getPaginatedQueryResponse(CatShardsResponse catShardsResponse) {
        return catShardsResponse.getPaginationStrategyInstance().getPaginatedQueryResponse();
    }

    @Override
    public List<ShardRouting> getShardRoutingResponseList(CatShardsResponse catShardsResponse, ClusterStateResponse clusterStateResponse) {
        return catShardsResponse.getPaginationStrategyInstance().getElementsFromRequestedToken();
    }
}
