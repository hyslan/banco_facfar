#include <iostream>
#include <windows.h>
#include <atlbase.h>

#import "C:\\Program Files\\Microsoft Office\root\\Office16\\EXCEL.EXE\\EXCEL.EXE" no_namespace

int main()
{
    // Inicialize o COM (Component Object Model)
    CoInitialize(NULL);

    try
    {
        // Crie uma instância do Excel
        Excel::_ApplicationPtr pExcel;
        pExcel.CreateInstance(__uuidof(Excel::Application));

        // Abra o arquivo do Excel que contém a consulta Power Query
        Excel::WorkbooksPtr pWorkbooks = pExcel->Workbooks;
        Excel::WorkbookPtr pWorkbook = pWorkbooks->Open(L"C:\\Caminho\\Para\\Seu\\Arquivo.xlsx");

        // Atualize todas as conexões de consulta Power Query no arquivo
        pWorkbook->RefreshAll();

        // Salve as alterações e feche o arquivo
        pWorkbook->Save();
        pWorkbook->Close();

        // Feche o Excel
        pExcel->Quit();
    }
    catch (const _com_error &e)
    {
        std::cerr << "Erro: " << e.ErrorMessage() << std::endl;
    }

    // Libere recursos do COM
    CoUninitialize();

    return 0;
}
