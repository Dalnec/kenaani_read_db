# -*- mode: python ; coding: utf-8 -*-

block_cipher = None
added_files = [
         ( 'kulami/models.py', 'kulami' ),
         ( 'base/db.py', 'base' ),
         ( 'pseapi/api.py', 'pseapi' )
         ]

a = Analysis(['main.py'],
             pathex=['C:\\Users\\Dany\\Desktop\\Pechan_read_db ver 1.0 con conf'],
             binaries=[],
             datas=added_files,
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='Faqture',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True )
