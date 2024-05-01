##!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------
# Archivo: csv_transformer.py
# Capitulo: Flujo de Datos
# Autor(es): Aldo De La Rosa & Ricardo & Juan Pablo & Javier Vargas
# Version: 1.0.0 Abril 2024
# Descripción:
#
#   Este archivo define un procesador de datos que se encarga de transformar
#   y formatear el contenido de un archivo TXT
#-------------------------------------------------------------------------
from src.extractors.txt_extractor import TXTExtractor
from os.path import join
import luigi, os, json

class TXTTransformer(luigi.Task):
    def requires(self):
        return TXTExtractor()
    
    def run(self):
        result = []
        for file in self.input():
            with file.open() as txt_file:
                lines = txt_file.readlines()
                for line in lines:
                    entry = line.strip().split()
                    if not entry[0]:
                        continue

                    try:
                        quantity = float(entry[1])
                        price = float(entry[2])
                    except ValueError:
                        print(f"Error: No se pudo convertir '{entry[1]}' o '{entry[2]}' a float.")
                        continue

                    result.append(
                        {
                            "description": entry[0],
                            "quantity": entry[1],
                            "price": entry[2],
                            "total": float(entry[1]) * float(entry[2]),
                            "invoice": entry[3],
                            "provider": entry[4],
                            "country": entry[5]
                        }
                    )
        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = join(project_dir, "result")
        return luigi.LocalTarget(join(result_dir, "txt.json"))