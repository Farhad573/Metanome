/**
 * Copyright 2015-2016 by Metanome Project
 *
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
package de.metanome.backend.resources;

import de.metanome.backend.constants.Constants;
import de.metanome.backend.result_postprocessing.result_store.ResultsStoreHolder;
import de.metanome.backend.result_postprocessing.results.FunctionalDependencyResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.*;

/**
 * Provides access to visualization JSON artifacts generated during result post-processing.
 *
 * Motivation: In some deployments, UI static resources under `/visualization/**` are not served
 * by the backend. The legacy UI relied on `visualization/FDResultAnalyzer/PrefixTree.json`.
 */
@Path("visualization")
public class VisualizationResource {

  @GET
  @Path("/fd/prefix-tree")
  @Produces(Constants.APPLICATION_JSON_RESOURCE_PATH)
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Response getFunctionalDependencyPrefixTree() {
    try {
      // Build legacy-compatible prefix tree JSON directly from currently loaded FD results.
      // This avoids relying on a generated file being present on disk/classpath.
      Object storeObj = ResultsStoreHolder.getStore("Functional Dependency");
      if (!(storeObj instanceof java.util.List) && storeObj == null) {
        // fall through to 404 below
      }
      // ResultsStoreHolder stores ResultsStore instances, but json-simple triggers rawtype warnings;
      // keep warning suppression localized to this endpoint.
      de.metanome.backend.result_postprocessing.result_store.ResultsStore store =
          (de.metanome.backend.result_postprocessing.result_store.ResultsStore) storeObj;
      if (store == null || store.list() == null || store.list().isEmpty()) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("No Functional Dependency results loaded")
            .build();
      }

      List<FunctionalDependencyResult> results = (List<FunctionalDependencyResult>) store.list();

      JSONObject root = new JSONObject();
      root.put("name", "FunctionalDependencies");
      // We don't have table size without data-dependent table stats here; keep it absent.

      // Group by dependant column name
      Map<String, List<FunctionalDependencyResult>> byDep = new TreeMap<>();
      for (FunctionalDependencyResult r : results) {
        if (r == null || r.getDependant() == null) continue;
        String depName = r.getDependant().getColumnIdentifier();
        if (depName == null) continue;
        byDep.computeIfAbsent(depName, k -> new ArrayList<>()).add(r);
      }

      JSONArray children = new JSONArray();
      for (Map.Entry<String, List<FunctionalDependencyResult>> e : byDep.entrySet()) {
        String dependant = e.getKey();
        List<FunctionalDependencyResult> deps = e.getValue();

        // Determinants become paths in a trie
        TrieNode trieRoot = new TrieNode(dependant);
        for (FunctionalDependencyResult r : deps) {
          if (r.getDeterminant() == null || r.getDeterminant().getColumnIdentifiers() == null) continue;
          List<String> detCols = new ArrayList<>();
          r.getDeterminant().getColumnIdentifiers().forEach(ci -> {
            if (ci != null && ci.getColumnIdentifier() != null) detCols.add(ci.getColumnIdentifier());
          });
          Collections.sort(detCols);
          trieRoot.addPath(detCols);
        }

        children.add(trieRoot.toJson(false));
      }
      root.put("children", children);

      return Response.ok(root.toJSONString()).build();
    } catch (Exception e) {
      e.printStackTrace();
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity("Could not load FD prefix tree visualization data")
          .build();
    }
  }

  // Minimal trie for determinant column combinations
  private static class TrieNode {
    final String name;
    final Map<String, TrieNode> children = new TreeMap<>();

    TrieNode(String name) {
      this.name = name;
    }

    void addPath(List<String> cols) {
      TrieNode cur = this;
      for (String c : cols) {
        if (c == null || c.trim().isEmpty()) continue;
        cur = cur.children.computeIfAbsent(c, TrieNode::new);
      }
    }

    @SuppressWarnings("unchecked")
    JSONObject toJson(boolean includeSize) {
      JSONObject o = new JSONObject();
      o.put("name", name);
      if (includeSize) {
        // Legacy circle packing uses `size`; without stats we use a constant.
        o.put("size", 1);
      }
      if (!children.isEmpty()) {
        JSONArray arr = new JSONArray();
        for (TrieNode child : children.values()) {
          arr.add(child.toJson(includeSize));
        }
        o.put("children", arr);
      }
      return o;
    }
  }
}
