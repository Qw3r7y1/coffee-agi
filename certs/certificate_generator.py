import os
import uuid
from datetime import datetime

from loguru import logger
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import inch, mm
from reportlab.pdfgen import canvas


class CertificateGenerator:
    def __init__(self, output_dir: str = "data/certificates"):
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        self._certs: dict = {}
        self._student_certs: dict[str, list] = {}

    def generate(
        self,
        student_name: str,
        student_email: str,
        topic: str,
        score: float,
        certificate_track: str,
    ) -> dict:
        cert_id = str(uuid.uuid4())
        issued_date = datetime.utcnow().strftime("%B %d, %Y")
        filepath = os.path.join(self.output_dir, f"{cert_id}.pdf")

        try:
            self._draw(filepath, student_name, certificate_track, score, issued_date, cert_id)
        except Exception as e:
            logger.error(f"Certificate generation error: {e}")
            return {"error": str(e)}

        cert = {
            "id": cert_id,
            "student_name": student_name,
            "student_email": student_email,
            "topic": topic,
            "certificate_track": certificate_track,
            "score": score,
            "issued_date": issued_date,
            "file_path": filepath,
        }
        self._certs[cert_id] = cert
        self._student_certs.setdefault(student_email, []).append(cert)
        logger.info(f"Certificate generated: {cert_id} for {student_name} ({certificate_track})")
        return cert

    def get_certificate(self, cert_id: str) -> dict | None:
        return self._certs.get(cert_id)

    def get_student_certificates(self, email: str) -> list:
        return self._student_certs.get(email, [])

    # ── Drawing ───────────────────────────────────────────────────────────────

    def _draw(
        self,
        filepath: str,
        name: str,
        track: str,
        score: float,
        date: str,
        cert_id: str,
    ):
        w, h = landscape(A4)
        c = canvas.Canvas(filepath, pagesize=landscape(A4))

        # Cream background
        c.setFillColorRGB(0.988, 0.961, 0.918)
        c.rect(0, 0, w, h, fill=1, stroke=0)

        # Coffee-brown outer border
        BROWN = (0.38, 0.19, 0.04)
        DARK = (0.22, 0.10, 0.01)
        GOLD = (0.72, 0.55, 0.10)

        c.setStrokeColorRGB(*BROWN)
        c.setLineWidth(7)
        c.rect(15, 15, w - 30, h - 30, stroke=1, fill=0)
        c.setLineWidth(1.5)
        c.setStrokeColorRGB(*GOLD)
        c.rect(25, 25, w - 50, h - 50, stroke=1, fill=0)

        # Corner ornaments
        for cx, cy, flip_x, flip_y in [
            (40, 40, 1, 1),
            (w - 40, 40, -1, 1),
            (40, h - 40, 1, -1),
            (w - 40, h - 40, -1, -1),
        ]:
            c.saveState()
            c.translate(cx, cy)
            c.scale(flip_x, flip_y)
            c.setStrokeColorRGB(*GOLD)
            c.setLineWidth(1)
            c.arc(-10, -10, 10, 10, 0, 90)
            c.line(10, 0, 25, 0)
            c.line(0, 10, 0, 25)
            c.restoreState()

        mid = w / 2

        # Brand header
        c.setFillColorRGB(*DARK)
        c.setFont("Helvetica-Bold", 13)
        c.drawCentredString(mid, h - 68, "MAILLARD COFFEE ROASTERS")
        c.setFont("Helvetica", 9)
        c.setFillColorRGB(*BROWN)
        c.drawCentredString(mid, h - 84, "Seed to Cup Intelligence  ·  Coffee AGI")

        # Thin rule
        c.setStrokeColorRGB(*GOLD)
        c.setLineWidth(0.8)
        c.line(mid - 160, h - 93, mid + 160, h - 93)

        # Main title
        c.setFillColorRGB(*DARK)
        c.setFont("Helvetica-Bold", 38)
        c.drawCentredString(mid, h - 148, "Certificate of Achievement")

        # Subtitle
        c.setFont("Helvetica", 13)
        c.setFillColorRGB(0.30, 0.15, 0.02)
        c.drawCentredString(mid, h - 180, "This is to certify that")

        # Student name
        c.setFont("Helvetica-BoldOblique", 32)
        c.setFillColorRGB(*BROWN)
        c.drawCentredString(mid, h - 225, name)

        # Underline name
        name_w = c.stringWidth(name, "Helvetica-BoldOblique", 32)
        c.setStrokeColorRGB(*BROWN)
        c.setLineWidth(0.8)
        c.line(mid - name_w / 2, h - 233, mid + name_w / 2, h - 233)

        # Body text
        c.setFont("Helvetica", 13)
        c.setFillColorRGB(0.20, 0.10, 0.01)
        c.drawCentredString(mid, h - 262, "has successfully completed")

        # Certificate track
        c.setFont("Helvetica-Bold", 20)
        c.setFillColorRGB(*DARK)
        c.drawCentredString(mid, h - 296, track)

        # Score badge
        c.setFillColorRGB(*GOLD)
        c.roundRect(mid - 70, h - 340, 140, 30, 6, fill=1, stroke=0)
        c.setFillColorRGB(*DARK)
        c.setFont("Helvetica-Bold", 13)
        c.drawCentredString(mid, h - 330, f"Score: {score:.1f}%  ·  Distinction" if score >= 90 else f"Score: {score:.1f}%  ·  Pass")

        # Date
        c.setFont("Helvetica", 11)
        c.setFillColorRGB(0.30, 0.15, 0.02)
        c.drawCentredString(mid, h - 362, f"Issued  {date}")

        # Signature line
        sig_x = mid - 80
        c.setStrokeColorRGB(*BROWN)
        c.setLineWidth(0.6)
        c.line(sig_x, h - 400, sig_x + 160, h - 400)
        c.setFont("Helvetica", 9)
        c.setFillColorRGB(*BROWN)
        c.drawCentredString(sig_x + 80, h - 412, "Maillard Coffee Roasters")

        # Footer
        c.setFont("Helvetica", 7)
        c.setFillColorRGB(0.55, 0.35, 0.12)
        c.drawCentredString(mid, 34, f"Certificate ID: {cert_id}   ·   Verify at maillardcoffee.com/verify")

        c.save()
