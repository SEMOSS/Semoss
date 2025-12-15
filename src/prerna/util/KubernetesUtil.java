package prerna.util;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.util.ClientBuilder;
import java.io.IOException;

public class KubernetesUtil {

    private static ApiClient client;
    private static String namespace;

    public static ApiClient getApiClient() throws IOException {
        if (client == null) {
            client = ClientBuilder.standard().build();
            Configuration.setDefaultApiClient(client);
            JSON.setLenientOnJson(true);
        }
        return client;
    }

    public static String getNamespace() {
        if (namespace == null) {
            namespace = Utility.getDIHelperProperty("kubernetes_namespace");
            if (namespace == null) {
                namespace = "default";
            }
        }
        return namespace;
    }
}
