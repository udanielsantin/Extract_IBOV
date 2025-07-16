FROM public.ecr.aws/lambda/python:3.9

# Instala dependências do sistema para Playwright/Chromium
RUN yum -y install libX11 libXcomposite libXcursor libXdamage libXext libXi libXtst cups-libs dbus-glib gtk3 alsa-lib

# Instala pip e dependências Python
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Instala o Chromium do Playwright
RUN python -m playwright install chromium

# Copia o código da lambda
COPY app.py ${LAMBDA_TASK_ROOT}

# Define o handler padrão
CMD ["app.lambda_handler"]
