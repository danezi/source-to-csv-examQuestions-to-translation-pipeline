"""
EXAKTE KOORDINATEN FÜR DIE ZWEI SEITEN FINDEN
==============================================

Dieses Skript hilft Ihnen, die GENAUEN Koordinaten zu finden,
um NUR die beiden Seiten mit dem Text zu erfassen.

KEINE Titelleiste, KEINE weißen Ränder.

Verwendung: python trouver_coordonnees_exactes.py
"""

from playwright.sync_api import sync_playwright
import time

#Buch 1 
#EMAIL = "lisibrinki"
#PASSWORT = "Buchholz25-"
#EBOOK_URL = "https://ebook.cornelsen.de/auth/220053730/ebook"

#Buch 2 aus dem Verlag das Buch für Zahnmedizin
#EMAIL = "lisa.baltzer@bbs-buchholz.de"
#PASSWORT = "Buchholz25-"
#EBOOK_URL = "https://ht-alex.de/courses"

#Buch 3
#EMAIL = "lisa.baltzer@bbs-buchholz.de"
#PASSWORT = "Buchholz25!-"
#EBOOK_URL = "https://www.westermann.de/meine-produkte"
#EBOOK_URL = "https://bibox2.westermann.de/v2/book/4886/page/4"


#Buch 4
EMAIL = "robert.westphal@bbs-buchholz.de"
PASSWORT = "herbst16"
EBOOK_URL = "https://www.merkur-medien.de/de/#/login/login?_k=x11bjo"


# STARTKOORDINATEN (Passen Sie diese an, bis es PERFEKT ist)
COORDONNEES_TEST = {
    'x': 230,       # Erhöhen = nach rechts verschieben
    'y': 20,       # Erhöhen = nach unten verschieben
    'width': 1455,  # Verringern = Ränder ausschließen
    'height': 1000   # Verringern = oben/unten ausschließen
}


def afficher_cadre(page, coords):
    """Zeigt einen roten Rahmen auf dem Bildschirm"""
    page.evaluate(f"""
        // Alten Rahmen entfernen
        document.getElementById('test-frame')?.remove();
        document.getElementById('test-label')?.remove();

        // Rahmen erstellen
        const frame = document.createElement('div');
        frame.id = 'test-frame';
        frame.style.position = 'fixed';
        frame.style.left = '{coords['x']}px';
        frame.style.top = '{coords['y']}px';
        frame.style.width = '{coords['width']}px';
        frame.style.height = '{coords['height']}px';
        frame.style.border = '5px solid red';
        frame.style.zIndex = '99999999';
        frame.style.pointerEvents = 'none';
        frame.style.boxShadow = '0 0 30px red, inset 0 0 30px rgba(255,0,0,0.3)';
        document.body.appendChild(frame);

        // Ecken markieren
        ['top-left', 'top-right', 'bottom-left', 'bottom-right'].forEach(pos => {{
            const corner = document.createElement('div');
            corner.style.position = 'absolute';
            corner.style.width = '40px';
            corner.style.height = '40px';
            corner.style.backgroundColor = 'red';
            if (pos.includes('top')) corner.style.top = '-5px';
            if (pos.includes('bottom')) corner.style.bottom = '-5px';
            if (pos.includes('left')) corner.style.left = '-5px';
            if (pos.includes('right')) corner.style.right = '-5px';
            frame.appendChild(corner);
        }});

        // Label
        const label = document.createElement('div');
        label.id = 'test-label';
        label.style.position = 'fixed';
        label.style.top = '{coords['y'] - 60}px';
        label.style.left = '{coords['x']}px';
        label.style.backgroundColor = 'red';
        label.style.color = 'white';
        label.style.padding = '15px 30px';
        label.style.fontSize = '20px';
        label.style.fontWeight = 'bold';
        label.style.zIndex = '99999999';
        label.style.borderRadius = '5px';
        label.style.boxShadow = '0 5px 15px rgba(0,0,0,0.5)';
        label.textContent = 'ERFASSUNGSZONE: {coords['width']}×{coords['height']}';
        document.body.appendChild(label);
    """)


def test_coordonnees():
    print("\n" + "="*70)
    print("  EXAKTE KOORDINATEN FÜR DIE SEITEN FINDEN")
    print("="*70 + "\n")

    coords = COORDONNEES_TEST.copy()

    print("📐 Aktuelle Koordinaten:")
    for key, val in coords.items():
        print(f"   {key:8s} = {val}")
    print()

    with sync_playwright() as p:
        print("🚀 Browser wird gestartet...")
        browser = p.chromium.launch(
            headless=False,
            args=['--force-device-scale-factor=1.0']
        )

        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            device_scale_factor=1.0
        )
        page = context.new_page()

        try:
            # Anmeldung
            print("🔐 Anmeldung...")
            page.goto(EBOOK_URL, wait_until="networkidle")
            time.sleep(2)

            try:
                page.fill('input[type="email"]', EMAIL)
                page.fill('input[type="password"]', PASSWORT)
                page.click('button[type="submit"]')
                time.sleep(3)
                print("   ✓ Angemeldet\n")
            except:
                print("   ⚠️ Manuelle Anmeldung erforderlich\n")

            # Anweisungen
            print("="*70)
            print("⏸️  VORBEREITUNG")
            print("="*70 + "\n")
            print("📋 BITTE JETZT:")
            print("   1. Navigieren Sie zu EINER Seite in Ihrem eBook")
            print("   2. Drücken Sie F11 (Vollbild)")
            print("   3. Stellen Sie sicher, dass 2 Seiten sichtbar sind")
            print("   4. Drücken Sie Strg+0 (Zoom 100%)")
            print("\n➡️  Drücken Sie dann ENTER hier...\n")
            input()

            # Zoom erzwingen
            page.keyboard.press("Control+0")
            time.sleep(1)

            # INTERAKTIVER MODUS - Koordinaten manuell anpassen
            while True:
                # Roten Rahmen anzeigen
                afficher_cadre(page, coords)

                print("\n" + "="*70)
                print("🔴 ROTER RAHMEN WIRD AUF DEM BILDSCHIRM ANGEZEIGT!")
                print("="*70 + "\n")

                print("📐 Aktuelle Koordinaten:")
                print(f"   x      = {coords['x']}")
                print(f"   y      = {coords['y']}")
                print(f"   width  = {coords['width']}")
                print(f"   height = {coords['height']}\n")

                print("🔍 ÜBERPRÜFEN SIE:")
                print("   ✅ Rahmen umschließt NUR die 2 Seiten")
                print("   ✅ KEINE Titelleiste sichtbar")
                print("   ✅ KEINE weißen Ränder links oder rechts")
                print("   ✅ Text beginnt direkt am Rahmenrand\n")

                print("="*70)
                print("⚙️  ANPASSUNGSOPTIONEN")
                print("="*70 + "\n")
                print("  1 = X - 10 (Rahmen nach LINKS)")
                print("  2 = X + 10 (Rahmen nach RECHTS)")
                print("  3 = Y - 10 (Rahmen nach OBEN)")
                print("  4 = Y + 10 (Rahmen nach UNTEN)")
                print("  5 = Breite - 20 (Rahmen SCHMALER)")
                print("  6 = Breite + 20 (Rahmen BREITER)")
                print("  7 = Höhe - 20 (Rahmen KLEINER)")
                print("  8 = Höhe + 20 (Rahmen GRÖSSER)")
                print("  f = FERTIG! Screenshot erstellen")
                print("  q = ABBRECHEN\n")

                wahl = input("➡️  Ihre Wahl: ").strip().lower()

                if wahl == '1':
                    coords['x'] -= 10
                    print("   → X um 10 verringert (nach links)")
                elif wahl == '2':
                    coords['x'] += 10
                    print("   → X um 10 erhöht (nach rechts)")
                elif wahl == '3':
                    coords['y'] -= 10
                    print("   → Y um 10 verringert (nach oben)")
                elif wahl == '4':
                    coords['y'] += 10
                    print("   → Y um 10 erhöht (nach unten)")
                elif wahl == '5':
                    coords['width'] -= 20
                    print("   → Breite um 20 verringert")
                elif wahl == '6':
                    coords['width'] += 20
                    print("   → Breite um 20 erhöht")
                elif wahl == '7':
                    coords['height'] -= 20
                    print("   → Höhe um 20 verringert")
                elif wahl == '8':
                    coords['height'] += 20
                    print("   → Höhe um 20 erhöht")
                elif wahl == 'f':
                    # Screenshot erstellen
                    print("\n📸 Erfassungszone wird aufgenommen...")
                    page.screenshot(
                        path="test_coordonnees_exactes.png",
                        clip=coords
                    )

                    print("\n" + "="*70)
                    print("✅ PERFEKT! SCREENSHOT ERSTELLT")
                    print("="*70 + "\n")

                    print("📂 Datei erstellt: test_coordonnees_exactes.png")
                    print("   → Überprüfen Sie diese Datei zur Bestätigung\n")

                    print("📋 FINALE KOORDINATEN ZUM KOPIEREN:")
                    print("="*70)
                    print("\nKopieren Sie dies in main_von_Bild_zu_word.py (Zeile ~200):\n")
                    print("    clip = {")
                    print(f"        'x': {coords['x']},")
                    print(f"        'y': {coords['y']},")
                    print(f"        'width': {coords['width']},")
                    print(f"        'height': {coords['height']}")
                    print("    }\n")
                    break
                elif wahl == 'q':
                    print("\n⚠️  Abgebrochen.")
                    break
                else:
                    print("\n⚠️  Ungültige Eingabe. Bitte wählen Sie 1-8, f oder q.")

                time.sleep(0.3)  # Kurze Pause vor Aktualisierung

            input("\nDrücken Sie ENTER zum Schließen...")

        except Exception as e:
            print(f"\n❌ Fehler: {e}")
            import traceback
            traceback.print_exc()

        finally:
            browser.close()

    print("\n✅ Test abgeschlossen!\n")


if __name__ == "__main__":
    test_coordonnees()
