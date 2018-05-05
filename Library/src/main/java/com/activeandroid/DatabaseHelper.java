package com.activeandroid;

/*
 * Copyright (C) 2010 Michael Pardo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import android.arch.persistence.db.SupportSQLiteDatabase;
import android.arch.persistence.db.SupportSQLiteOpenHelper;
import android.arch.persistence.db.framework.FrameworkSQLiteOpenHelperFactory;
import android.content.Context;
import android.text.TextUtils;

import com.activeandroid.util.IOUtils;
import com.activeandroid.util.Log;
import com.activeandroid.util.NaturalOrderComparator;
import com.activeandroid.util.SQLiteUtils;
import com.activeandroid.util.SqlParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class DatabaseHelper implements SupportSQLiteOpenHelper {
	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC CONSTANTS
	//////////////////////////////////////////////////////////////////////////////////////

	public final static String MIGRATION_PATH = "migrations";

	//////////////////////////////////////////////////////////////////////////////////////
    // PRIVATE FIELDS
    //////////////////////////////////////////////////////////////////////////////////////

    private final String mSqlParser;
    private final SupportSQLiteOpenHelper helper;

    //////////////////////////////////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////////////////////////////////

	public DatabaseHelper(com.activeandroid.Configuration configuration) {
		super();
		copyAttachedDatabase(configuration.getContext(), configuration.getDatabaseName());
		mSqlParser = configuration.getSqlParser();

        Factory factory = new FrameworkSQLiteOpenHelperFactory();
        Configuration config = Configuration.builder(configuration.getContext())
                .name(configuration.getDatabaseName())
                .callback(new Callback(configuration.getDatabaseVersion()) {
                    @Override
                    public void onCreate(SupportSQLiteDatabase db) {
                        DatabaseHelper.this.onCreate(db);
                    }

                    @Override
                    public void onUpgrade(SupportSQLiteDatabase db, int oldVersion, int newVersion) {
                        DatabaseHelper.this.onUpgrade(db, oldVersion, newVersion);
                    }

                    @Override
                    public void onOpen(SupportSQLiteDatabase db) {
                        DatabaseHelper.this.onOpen(db);
                    }
                })
                .build();

        helper = factory.create(config);
	}

	//////////////////////////////////////////////////////////////////////////////////////
	// OVERRIDEN METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	public void onOpen(SupportSQLiteDatabase db) {
		executePragmas(db);
	}

	public void onCreate(SupportSQLiteDatabase db) {
		executePragmas(db);
		executeCreate(db);
		executeMigrations(db, -1, db.getVersion());
		executeCreateIndex(db);
	}

	public void onUpgrade(SupportSQLiteDatabase db, int oldVersion, int newVersion) {
		executePragmas(db);
		executeCreate(db);
		executeMigrations(db, oldVersion, newVersion);
	}

	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	public void copyAttachedDatabase(Context context, String databaseName) {
		final File dbPath = context.getDatabasePath(databaseName);

		// If the database already exists, return
		if (dbPath.exists()) {
			return;
		}

		// Make sure we have a path to the file
		dbPath.getParentFile().mkdirs();

		// Try to copy database file
		try {
			final InputStream inputStream = context.getAssets().open(databaseName);
			final OutputStream output = new FileOutputStream(dbPath);

			byte[] buffer = new byte[8192];
			int length;

			while ((length = inputStream.read(buffer, 0, 8192)) > 0) {
				output.write(buffer, 0, length);
			}

			output.flush();
			output.close();
			inputStream.close();
		}
		catch (IOException e) {
			Log.e("Failed to open file", e);
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////
	// PRIVATE METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	private void executePragmas(SupportSQLiteDatabase db) {
		if (SQLiteUtils.FOREIGN_KEYS_SUPPORTED) {
			db.execSQL("PRAGMA foreign_keys=ON;");
			Log.i("Foreign Keys supported. Enabling foreign key features.");
		}
	}

	private void executeCreateIndex(SupportSQLiteDatabase db) {
		db.beginTransaction();
		try {
			for (TableInfo tableInfo : Cache.getTableInfos()) {
				String[] definitions = SQLiteUtils.createIndexDefinition(tableInfo);

				for (String definition : definitions) {
					db.execSQL(definition);
				}
			}
			db.setTransactionSuccessful();
		}
		finally {
			db.endTransaction();
		}
	}

	private void executeCreate(SupportSQLiteDatabase db) {
		db.beginTransaction();
		try {
			for (TableInfo tableInfo : Cache.getTableInfos()) {
				db.execSQL(SQLiteUtils.createTableDefinition(tableInfo));
			}
			db.setTransactionSuccessful();
		}
		finally {
			db.endTransaction();
		}
	}

	private boolean executeMigrations(SupportSQLiteDatabase db, int oldVersion, int newVersion) {
		boolean migrationExecuted = false;
		try {
			final List<String> files = Arrays.asList(Cache.getContext().getAssets().list(MIGRATION_PATH));
			Collections.sort(files, new NaturalOrderComparator());

			db.beginTransaction();
			try {
				for (String file : files) {
					try {
						final int version = Integer.valueOf(file.replace(".sql", ""));

						if (version > oldVersion && version <= newVersion) {
							executeSqlScript(db, file);
							migrationExecuted = true;

							Log.i(file + " executed succesfully.");
						}
					}
					catch (NumberFormatException e) {
						Log.w("Skipping invalidly named file: " + file, e);
					}
				}
				db.setTransactionSuccessful();
			}
			finally {
				db.endTransaction();
			}
		}
		catch (IOException e) {
			Log.e("Failed to execute migrations.", e);
		}

		return migrationExecuted;
	}

	private void executeSqlScript(SupportSQLiteDatabase db, String file) {

	    InputStream stream = null;

		try {
		    stream = Cache.getContext().getAssets().open(MIGRATION_PATH + "/" + file);

		    if (com.activeandroid.Configuration.SQL_PARSER_DELIMITED.equalsIgnoreCase(mSqlParser)) {
		        executeDelimitedSqlScript(db, stream);

		    } else {
		        executeLegacySqlScript(db, stream);

		    }

		} catch (IOException e) {
			Log.e("Failed to execute " + file, e);

		} finally {
		    IOUtils.closeQuietly(stream);

		}
	}

	private void executeDelimitedSqlScript(SupportSQLiteDatabase db, InputStream stream) throws IOException {

	    List<String> commands = SqlParser.parse(stream);

	    for(String command : commands) {
	        db.execSQL(command);
	    }
	}

	private void executeLegacySqlScript(SupportSQLiteDatabase db, InputStream stream) throws IOException {

	    InputStreamReader reader = null;
        BufferedReader buffer = null;

        try {
            reader = new InputStreamReader(stream);
            buffer = new BufferedReader(reader);
            String line = null;

            while ((line = buffer.readLine()) != null) {
                line = line.replace(";", "").trim();
                if (!TextUtils.isEmpty(line)) {
                    db.execSQL(line);
                }
            }

        } finally {
            IOUtils.closeQuietly(buffer);
            IOUtils.closeQuietly(reader);

        }
	}

    @Override
    public String getDatabaseName() {
        return helper.getDatabaseName();
    }

    @Override
    public void setWriteAheadLoggingEnabled(boolean enabled) {
        helper.setWriteAheadLoggingEnabled(enabled);
    }

    @Override
    public SupportSQLiteDatabase getWritableDatabase() {
        return helper.getWritableDatabase();
    }

    @Override
    public SupportSQLiteDatabase getReadableDatabase() {
        return helper.getReadableDatabase();
    }

    @Override
    public void close() {
        helper.close();
    }
}
