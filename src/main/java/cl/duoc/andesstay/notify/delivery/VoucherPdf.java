package cl.duoc.andesstay.notify.delivery;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class VoucherPdf {

	private VoucherPdf() {
	}

	static byte[] render(String documentType, String reservationId, String guestName) {
		String line = ascii(documentType + " " + reservationId + " " + guestName);
		String stream = "BT /F1 12 Tf 72 720 Td (" + line + ") Tj ET\n";
		List<String> objects = List.of(
				"<< /Type /Catalog /Pages 2 0 R >>",
				"<< /Type /Pages /Count 1 /Kids [3 0 R] >>",
				"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
				"<< /Length " + stream.length() + " >>\nstream\n" + stream + "endstream",
				"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>");
		StringBuilder pdf = new StringBuilder("%PDF-1.4\n");
		List<Integer> offsets = new ArrayList<>();
		for (int index = 0; index < objects.size(); index++) {
			offsets.add(pdf.toString().getBytes(StandardCharsets.US_ASCII).length);
			pdf.append(index + 1).append(" 0 obj\n").append(objects.get(index)).append("\nendobj\n");
		}
		int xref = pdf.toString().getBytes(StandardCharsets.US_ASCII).length;
		pdf.append("xref\n0 ").append(objects.size() + 1).append('\n');
		pdf.append("0000000000 65535 f \n");
		for (int offset : offsets) {
			pdf.append(String.format("%010d 00000 n \n", offset));
		}
		pdf.append("trailer << /Size ").append(objects.size() + 1).append(" /Root 1 0 R >>\n");
		pdf.append("startxref\n").append(xref).append("\n%%EOF\n");
		return pdf.toString().getBytes(StandardCharsets.US_ASCII);
	}

	private static String ascii(String value) {
		StringBuilder out = new StringBuilder();
		for (char character : value.toCharArray()) {
			if (character == '\\' || character == '(' || character == ')') {
				out.append('\\').append(character);
			} else if (character >= 32 && character < 127) {
				out.append(character);
			} else {
				out.append('?');
			}
		}
		return out.toString();
	}
}
