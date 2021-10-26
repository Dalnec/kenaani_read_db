# Crear entorno virtual
Esto lo haces dentro de tu directorio de trabajo

```
python3 -m venv venv
venv/Scripts/activate.bat  #para cmd
venv/Scripts/Activate.ps1  #para powershell
```

# Instalar los requerimientos

```
pip install -r requirements.txt
pip freeze > requirements.txt
```

# Actualizar pip
```
python -m pip install --upgrade pip
```

```
pip install nombre_de_paquete
```

## ESTADOS FACTURAS 
```
	estado_declaracion				estado_declaracion_anulado
	1. PENDIENTE 				-		"
	2. PROCESADO 				- 		"
```

## ESTADOS FACTURAS ANULADAS
```
	estado_declaracion				estado_declaracion_anulado
	1. ANULADO	 				-		PENDIENTE
	2. ANULADO PENDIENTE 		- 		PENDIENTE
	3. ANULACION A CONSULTAR 	- 		PENDIENTE
	4. ANULADO PROCESADO		- 		PROCESADO
```

## ESTADOS BOLETAS 
```
	estado_declaracion				estado_declaracion_anulado
	1. PENDIENTE 				-		"
	2. POR RESUMIR 				- 		"
	3. POR CONSULTAR			-		"
	4. PROCESADO				-		"
```

## ESTADOS BOLETAS ANULADAS
```
	estado_declaracion				estado_declaracion_anulado
	1. ANULADO 					-		PENDIENTE
	2. ANULADO POR RESUMIR 		- 		PENDIENTE
	3. ANULADO POR CONSULTAR	-		PENDIENTE
	4. POR ANULAR				-		PENDIENTE
	5. ANULACION A CONSULTAR	-		PENDIENTE
	6. ANULADO	PROCESADO		-		PROCESADO
```

## ESTADOS CPE BOLETAS
```
	state_type_id				state_type_description
	1.		01			-			Registrado
	2. 		03 			- 			Enviado
	3. 		05			-			Aceptado
```

## ESTADOS CPE BOLETAS ANULADAS
```
	state_type_id				state_type_description
	1.		01			-			Por Anular
	2. 		03 			- 			Anulado
```