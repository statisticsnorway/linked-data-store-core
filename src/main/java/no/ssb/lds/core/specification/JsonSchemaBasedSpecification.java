package no.ssb.lds.core.specification;

import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElement;
import no.ssb.lds.core.schema.JsonSchema;
import no.ssb.lds.core.schema.JsonSchema04Builder;
import no.ssb.lds.core.schema.SchemaRepository;
import no.ssb.lds.core.utils.FileAndClasspathReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

public class JsonSchemaBasedSpecification implements Specification, SchemaRepository {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaBasedSpecification.class);

    public static JsonSchemaBasedSpecification create(String... specificationSchema) {
        JsonSchema jsonSchema = null;
        StringBuilder sb = new StringBuilder();
        for (String schemaPathStr : specificationSchema) {
            Path schemaPath = Paths.get(schemaPathStr);
            if (schemaPath.toFile().isDirectory()) {
                // directory
                Pattern endsWithJsonPattern = Pattern.compile("(.*)[.][Jj][Ss][Oo][Nn]");
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(schemaPath)) {
                    Iterator<Path> it = stream.iterator();
                    while (it.hasNext()) {
                        Path filePath = it.next();
                        if (endsWithJsonPattern.matcher(filePath.toFile().getName()).matches()) {
                            jsonSchema = schemaFromFile(jsonSchema, sb, filePath);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                jsonSchema = schemaFromFile(jsonSchema, sb, schemaPath);
            }
        }
        LOG.info("{}", (sb.length() == 0 ? "No schemas configured!" : "Managed domains: " + sb.toString().substring(1)));
        return SpecificationJsonSchemaBuilder.createBuilder(jsonSchema).build();
    }

    private static JsonSchema schemaFromFile(JsonSchema jsonSchema, StringBuilder sb, Path path) {
        String json = FileAndClasspathReaderUtils.readFileOrClasspathResource(path.toString());
        if (json == null) {
            throw new IllegalArgumentException("Unable to find resource: " + path.toString());
        }
        String filename = path.toFile().getName();
        String schemaFilename = filename.substring(filename.lastIndexOf("/") + 1);
        String managedDomain = schemaFilename.substring(0, schemaFilename.length() - ".json".length());
        sb.append(" /" + managedDomain);
        jsonSchema = new JsonSchema04Builder(jsonSchema, managedDomain, json).build();
        return jsonSchema;
    }

    private final JsonSchema jsonSchema;

    private final SpecificationElement root;

    public JsonSchemaBasedSpecification() {
        this.jsonSchema = null;
        this.root = null;
    }

    public JsonSchemaBasedSpecification(JsonSchema jsonSchema, SpecificationElement root) {
        this.jsonSchema = jsonSchema;
        this.root = root;
    }

    @Override
    public SpecificationElement getRootElement() {
        return root;
    }

    @Override
    public Set<String> getManagedDomains() {
        if (jsonSchema == null) {
            return Collections.emptySet();
        }
        return jsonSchema.getSchemaNames();
    }

    @Override
    public JsonSchema getJsonSchema() {
        return jsonSchema;
    }

}
