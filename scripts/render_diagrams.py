#!/usr/bin/env python3
"""Renderiza os diagramas do README a partir do proprio README.

O README e a fonte unica: cada bloco ```mermaid dentro de um <details> vira um
PNG em assets/. Nao ha copia .mmd no repo de proposito -- duas copias divergem,
e um diagrama que nao bate com a legenda e pior que nenhum.

Uso:
    make diagrams

Requer `npx` (baixa o @mermaid-js/mermaid-cli sob demanda). O Chromium do
puppeteer roda com --no-sandbox porque em Ubuntu 23.10+ o namespace de usuario
sem privilegio vem desabilitado por AppArmor e o launch falha sem isso.
"""

import re
import subprocess
import sys
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
README = RAIZ / "README.md"
ASSETS = RAIZ / "assets"

# Ordem dos blocos ```mermaid no README -> arquivo de saida.
SAIDAS = ["architecture-ingestion.png", "architecture-snowflake.png"]

FUNDO = "#12161c"  # igual ao 'background' do themeVariables dos diagramas
LARGURA = "2400"


def main() -> int:
    blocos = re.findall(r"```mermaid\n(.*?)\n```", README.read_text(encoding="utf-8"), re.S)
    if len(blocos) != len(SAIDAS):
        print(f"ERRO: {len(blocos)} blocos mermaid no README, {len(SAIDAS)} saidas declaradas.")
        print("Atualize SAIDAS neste script quando acrescentar ou remover um diagrama.")
        return 1

    ASSETS.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        pptr = Path(tmp) / "pptr.json"
        pptr.write_text('{"args":["--no-sandbox","--disable-setuid-sandbox"]}')

        for bloco, nome in zip(blocos, SAIDAS, strict=True):
            entrada = Path(tmp) / f"{nome}.mmd"
            entrada.write_text(bloco, encoding="utf-8")
            destino = ASSETS / nome
            subprocess.run(
                ["npx", "-y", "@mermaid-js/mermaid-cli", "-p", str(pptr),
                 "-i", str(entrada), "-o", str(destino), "-w", LARGURA, "-b", FUNDO],
                check=True,
            )
            print(f"  {destino.relative_to(RAIZ)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
