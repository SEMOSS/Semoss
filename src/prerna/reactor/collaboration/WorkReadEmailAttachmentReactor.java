package prerna.reactor.collaboration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

import prerna.collaboration.EmailAttachmentFiles;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Editor-only file transport. Not registered as an agent tool. */
public class WorkReadEmailAttachmentReactor extends AbstractCollaborationReactor {

	public WorkReadEmailAttachmentReactor() {
		this.keysToGet = new String[] { "path", "sha256" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		getUser();
		if (this.insight.getRoomId() == null) throw new IllegalArgumentException("Open the email's room first.");
		String reference = getString("path");
		if (reference == null || !reference.startsWith(EmailAttachmentFiles.DIRECTORY + "/")) {
			throw new IllegalArgumentException("Expected a prepared email attachment.");
		}
		try {
			Path root = Path.of(this.insight.getInsightFolder()).toRealPath();
			Path file = MCPUtility.resolveContainedMcpFile(root.toString(), reference);
			if (!file.startsWith(root.resolve(EmailAttachmentFiles.DIRECTORY))) {
				throw new IllegalArgumentException("Expected a prepared email attachment.");
			}
			byte[] bytes = EmailAttachmentFiles.read(file);
			if (!EmailAttachmentFiles.digest(bytes).equals(getString("sha256"))) {
				throw new IllegalArgumentException("The email attachment changed. Ask the assistant to attach it again.");
			}
			return mapResult(Map.of("path", reference, "name", file.getFileName().toString(), "size", bytes.length,
					"contentBase64", Base64.getEncoder().encodeToString(bytes)));
		} catch (IOException e) {
			throw new IllegalArgumentException("The email attachment could not be read. Ask the assistant to attach it again.", e);
		}
	}
}
