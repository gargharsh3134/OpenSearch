/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.annotation.ExperimentalApi;

@ExperimentalApi
public class RemoteReadResultsVerbose<T> {
    T obj;
    String component;
    String componentName;
    final long readMS;
    final long serdeMS;

    public RemoteReadResultsVerbose(T obj, String component, String componentName, long readMS, long serdeMS) {
        this.obj = obj;
        this.component = component;
        this.componentName = componentName;
        this.readMS = readMS;
        this.serdeMS = serdeMS;
    }

    public T getObj() {
        return obj;
    }

    public String getComponent() {
        return component;
    }

    public String getComponentName() {
        return componentName;
    }

    public long getReadMS() {
        return readMS;
    }

    public long getSerdeMS() {
        return serdeMS;
    }
}
