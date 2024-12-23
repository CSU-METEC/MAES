pushd ..

python src/DataDictionaryGenerator.py -c config/dataDictionaryConfig.json

python src/GenerateArgDoc.py -h

pandoc README_MEET2.md -o README_MEET2.html

popd

pandoc reference/CombustionCalculations.docx -o source/PhysicalProcesses/reference/CombustionCalculations.rst

pandoc reference/CompressorEngineLoad.docx -o source/ModelReference/reference/CompressorEngineLoad.rst

pandoc reference/TankBatt.tex -o source/ModelReference/reference/TankBattRef.rst

pandoc reference/WellsReference.tex -o source/PhysicalProcesses/reference/WellsReference.rst

pandoc reference/leakWord.docx -o source/PhysicalProcesses/reference/LeakReference.rst

call make clean

call make html
