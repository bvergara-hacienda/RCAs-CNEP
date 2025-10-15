# Extracción y Clasificación de Obligaciones Ambientales desde RCAs

Este repositorio contiene los códigos utilizados para la extracción y estandarización de obligaciones ambientales a partir de las Resoluciones de Calificación Ambiental (RCA) emitidas por el Servicio de Evaluación Ambiental (SEA) de Chile, empleando técnicas de Inteligencia Artificial.

---

## Estructura del Repositorio

El repositorio está organizado en dos carpetas principales:

- `codigo_principal`  
  Contiene la metodología principal, que combina herramientas gratuitas de OCR y procesamiento de texto con modelos de lenguaje (LLMs) para extraer y clasificar las obligaciones ambientales. Esta metodología genera bases intermedias con considerandos y subconsiderandos organizados, listas para su interpretación por el modelo de IA.

- `codigo_alternativo`  
  Contiene la metodología alternativa, que utiliza Amazon Textract para la extracción estructurada de texto y tablas desde PDF, seguida de la interpretación de obligaciones mediante LLMs. Esta vía permite comparar resultados y evaluar consistencia frente a la metodología principal.

---



## Flujo de Trabajo

El proceso general se divide en dos pasos:

1. **Extracción de información desde PDF:**  
   - Lectura de documentos escaneados o digitales.  
   - Identificación de considerandos relevantes y subconsiderandos.  
   - Extracción de tablas y textos en formatos estructurados.  

2. **Interpretación y clasificación de obligaciones:**  
   - Aplicación de modelos de lenguaje (GPT) para identificar y clasificar obligaciones.  
   - Consolidación y deduplicación de datos.  
   - Generación de archivos finales estandarizados por considerando y obligación.

Ambas metodologías convergen en esta etapa, aunque difieren en el insumo que procesan.

---

## Resultados

Los resultados finales de ambas metodologías se encuentran disponibles en Drive:  
[📂 Acceder a los resultados](https://drive.google.com/drive/folders/1Gy96X38YkbMpZLdYdguJUGaFGxNfXbwA?usp=sharing)

---

## Requisitos y Dependencias

Algunas librerías utilizadas incluyen, entre otras:

- `PyMuPDF` (`fitz`) para lectura de PDFs.  
- `pandas` y `numpy` para manejo de datos.  
- `selenium` para descarga automatizada de documentos.  
- `docTR` para OCR de PDFs escaneados.  
- `tabula` para extracción de tablas desde PDFs.  
- `openai` o `google.generativeai` para procesamiento con LLMs.  

Se recomienda utilizar **Python ≥ 3.10** y un entorno virtual o Anaconda para gestionar dependencias.

---

## Notas

- La metodología principal se utilizó como base del estudio por su mayor desempeño práctico, aunque ambas metodologías ofrecen resultados consistentes.  
- El repositorio incluye manejo de errores y procesamiento robusto de documentos heterogéneos, pero los archivos muy complejos o escaneados pueden requerir revisión manual.  
- Se sugiere revisar el flujo de ejecución y los notebooks de ejemplo antes de aplicar los códigos a nuevos archivos.

---

## Contacto


Para dudas o sugerencias: bvergara@hacienda.gov.cl

