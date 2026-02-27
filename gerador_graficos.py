import pandas as pd
import matplotlib.pyplot as plt


def gerar_graficos(df, nome_df, coluna_tempo):

    for tipo in ['sensor', 'gateway']:

        df_tipo = df[df['tipo'] == tipo].copy()

        if df_tipo.empty:
            continue

        # Tratamento da coluna de tempo
        if coluna_tempo == 'mes_ano':
            df_tipo['data'] = pd.to_datetime(df_tipo['mes_ano'], format='%m_%Y')
        else:
            df_tipo['data'] = pd.to_datetime(df_tipo['ano'], format='%Y')

        df_tipo = df_tipo.sort_values('data')

        # =====================================================
        # CASO CORREDOR
        # =====================================================
        if 'corredor' in df_tipo.columns:

            # ---------------- MTTR ----------------
            plt.figure(figsize=(10, 5))

            for corredor in df_tipo['corredor'].unique():
                df_corr = df_tipo[df_tipo['corredor'] == corredor]
                plt.plot(df_corr['data'], df_corr['mttr'], marker='o', label=corredor)

            plt.title(f"{nome_df} - {tipo} - MTTR")
            plt.xticks(rotation=45)
            plt.legend()
            plt.tight_layout()

            nome_arquivo = f"{nome_df}_{tipo}_mttr.png"
            plt.savefig(nome_arquivo)
            plt.close()

            print(f"Salvo: {nome_arquivo}")

            # ---------------- MTBF ----------------
            plt.figure(figsize=(10, 5))

            for corredor in df_tipo['corredor'].unique():
                df_corr = df_tipo[df_tipo['corredor'] == corredor]
                plt.plot(df_corr['data'], df_corr['mtbf'], marker='o', label=corredor)

            plt.title(f"{nome_df} - {tipo} - MTBF")
            plt.xticks(rotation=45)
            plt.legend()
            plt.tight_layout()

            nome_arquivo = f"{nome_df}_{tipo}_mtbf.png"
            plt.savefig(nome_arquivo)
            plt.close()

            print(f"Salvo: {nome_arquivo}")

        # =====================================================
        # CASO BARRAGEM (comentado para uso futuro)
        # =====================================================
        """
        if 'barragem' in df_tipo.columns:

            for barragem in df_tipo['barragem'].unique():

                df_barr = df_tipo[df_tipo['barragem'] == barragem]

                if df_barr.empty:
                    continue

                plt.figure(figsize=(10, 5))
                plt.plot(df_barr['data'], df_barr['mttr'], marker='o', label="MTTR")
                plt.plot(df_barr['data'], df_barr['mtbf'], marker='o', label="MTBF")

                plt.title(f"{nome_df} - {tipo} - {barragem}")
                plt.xticks(rotation=45)
                plt.legend()
                plt.tight_layout()

                nome_limpo = str(barragem).replace(" ", "_")
                nome_arquivo = f"{nome_df}_{tipo}_{nome_limpo}.png"

                plt.savefig(nome_arquivo)
                plt.close()

                print(f"Salvo: {nome_arquivo}")
        """