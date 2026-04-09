package de.metanome.backend.resources;

import de.metanome.backend.dpql.DpqlExpandRequest;
import de.metanome.backend.dpql.DpqlExpandResponseDto;
import de.metanome.backend.dpql.DpqlNormalizedTablePageResponseDto;
import de.metanome.backend.dpql.DpqlQuerryRequest;
import de.metanome.backend.dpql.DpqlRunStatusDto;
import de.metanome.backend.dpql.DpqlService;
import de.metanome.backend.dpql.result.ResultReader;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/dpql")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DpqlResource {
    private final DpqlService dpqlService = new DpqlService();
    private static final String RESULTS_DIR = "results"; // Keep in sync with DpqlService

    @GET
    @Path("/runs")
    public Response listRuns() {
        try {
            List<Map<String, Object>> runs = dpqlService.listRuns();
            return Response.ok(runs).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error listing DPQL runs: " + t.getMessage())
              .build();
        }
    }

    @GET
    @Path("/runs/{id}")
    public Response getRun(@PathParam("id") String executionId) {
        try {
            Map<String, Object> run = dpqlService.getRun(executionId);
            if (run == null) {
                return Response.status(Response.Status.NOT_FOUND).entity("Execution ID not found").build();
            }
            return Response.ok(run).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error reading DPQL run: " + t.getMessage())
              .build();
        }
    }

    @DELETE
    @Path("/runs/{id}")
    public Response deleteRun(@PathParam("id") String executionId) {
        try {
            boolean ok = dpqlService.deleteRun(executionId);
            if (!ok) {
                return Response.status(Response.Status.NOT_FOUND).entity("Execution ID not found").build();
            }
            return Response.ok(Collections.singletonMap("deleted", true)).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error deleting DPQL run: " + t.getMessage())
              .build();
        }
    }

    @GET
    @Path("/runs/{id}/normalized-table/{tableId}")
    public Response getNormalizedTablePage(
            @PathParam("id") String executionId,
            @PathParam("tableId") long tableId,
            @QueryParam("offset") @DefaultValue("0") int offset,
            @QueryParam("limit") @DefaultValue("100") int limit,
            @QueryParam("search") String search) {
        try {
            DpqlNormalizedTablePageResponseDto page = dpqlService.getNormalizedTablePage(executionId, tableId, offset, limit, search);
            if (page == null) {
                return Response.status(Response.Status.NOT_FOUND).entity("Execution ID or tableId not found").build();
            }
            return Response.ok(page).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
              .entity(e.getMessage())
              .build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error reading normalized table page: " + t.getMessage())
              .build();
        }
    }

    @POST
    @Path("/execute")
    public Response executeToDisk(DpqlQuerryRequest request) {
        if (request == null || request.getQuery() == null || request.getQuery().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("Query cannot be null or empty")
                    .build();
        }
        if (request.getEngineId() == null && (request.getEngineFileName() == null || request.getEngineFileName().trim().isEmpty())) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("Engine must be selected (engineId or engineFileName)")
                    .build();
        }
        try {
            String executionId = dpqlService.executeToDisk(request);
            return Response.ok(Collections.singletonMap("executionId", executionId)).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(e.getMessage())
                    .build();
        } catch (Throwable t) {
            t.printStackTrace();
            String message = t.getClass().getName() + (t.getMessage() != null ? (": " + t.getMessage()) : "");
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("error", message);
            body.put("stackTrace", toTrimmedStackTrace(t, 12000));
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(body)
                    .build();
        }
    }

    @GET
    @Path("/runs/{id}/status")
    public Response getRunStatus(@PathParam("id") String executionId) {
        try {
            DpqlRunStatusDto status = dpqlService.getRunStatus(executionId);
            if (status == null) {
                return Response.status(Response.Status.NOT_FOUND).entity("Execution ID not found").build();
            }
            return Response.ok(status).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error reading DPQL run status: " + t.getMessage())
              .build();
        }
    }

    @POST
    @Path("/runs/{id}/cancel")
    public Response cancelRun(@PathParam("id") String executionId) {
        try {
            boolean ok = dpqlService.cancelRun(executionId);
            if (!ok) {
                return Response.status(Response.Status.BAD_REQUEST).entity("Run not cancelable").build();
            }
            return Response.ok(Collections.singletonMap("status", "CANCELED")).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error canceling DPQL run: " + t.getMessage())
              .build();
        }
    }

    @GET
    @Path("/results/{id}")
    public Response getResultOverview(@PathParam("id") String executionId) {
        try {
            Map<String, Object> result = dpqlService.getResultOverview(executionId);
            if (result == null) {
                return Response.status(Response.Status.NOT_FOUND).entity("Execution ID not found").build();
            }
            return Response.ok(result).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error reading results: " + e.getMessage())
                    .build();
        }
    }

    @GET
    @Path("/results/{id}/table/{tableId}")
    public Response getTablePage(
            @PathParam("id") String executionId,
            @PathParam("tableId") int tableId,
            @QueryParam("offset") @DefaultValue("0") int offset,
            @QueryParam("limit") @DefaultValue("100") int limit,
            @QueryParam("search") String search) {
        try {
            Map<String, Object> result = dpqlService.getTablePage(executionId, tableId, offset, limit, search);
            if (result == null) {
                return Response.status(Response.Status.NOT_FOUND).entity("Execution ID or Table ID not found").build();
            }
            return Response.ok(result).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error reading table page: " + e.getMessage())
                    .build();
        }
    }

    @GET
    @Path("/results/{id}/table/{tableId}/export")
    @Produces("text/csv")
    public Response exportTableCsv(
            @PathParam("id") String executionId,
            @PathParam("tableId") int tableId,
            @QueryParam("search") String search) {
        try {
            ResultReader reader = new ResultReader(RESULTS_DIR, executionId);
            if (!reader.exists()) {
                return Response.status(Response.Status.NOT_FOUND).entity("Execution ID not found").build();
            }

            StreamingOutput stream = (out) -> reader.writeTableCsv(tableId, search, out);
            String filename = "dpql-" + executionId + "-table-" + tableId + ".csv";
            return Response.ok(stream)
                    .type("text/csv; charset=utf-8")
                    .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                    .header("Cache-Control", "no-store")
                    .build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error exporting table: " + t.getMessage())
                    .build();
        }
    }

    @POST
    @Path("/expand")
    public Response expand(DpqlExpandRequest request) {
        try {
            DpqlExpandResponseDto dto = dpqlService.expand(request);
            return Response.ok(dto).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
              .entity(e.getMessage())
              .build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity("Error expanding pattern: " + t.getMessage())
              .build();
        }
    }

    private static String toTrimmedStackTrace(Throwable t, int maxChars) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        String full = sw.toString();
        if (full.length() <= maxChars) {
            return full;
        }
        return full.substring(0, maxChars) + "\n... (trimmed)";
    }
}
