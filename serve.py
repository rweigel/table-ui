import tableui
import utilrsw.uvicorn

configs = tableui.cli()

utilrsw.uvicorn.run("tableui.app", configs)
