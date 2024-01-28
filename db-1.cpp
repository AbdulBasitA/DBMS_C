/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif


int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
    rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) &&
					((cur->next != NULL) && (cur->next->tok_value == S_STAR)) &&
					((cur->next->next != NULL) && (cur->next->next->tok_value == K_FROM)))
	{
		printf("SELECT * FROM statement\n");
		cur_cmd = SELECT;
		cur = cur->next->next->next;
	}
	else if ((cur->tok_value == K_SELECT) && (cur->next != NULL))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
	}
					
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DELETE) &&
					((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("DELETE TABLE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_UPDATE) &&
					((cur->next != NULL)))
	{
		printf("UPDATE TABLE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else
  {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case INSERT:
						rc = sem_insert_into_table(cur);
						break;
			case SELECT:
						rc = sem_select(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case DELETE:
						rc = sem_delete(cur);
						break;
			case UPDATE:
						rc = sem_update(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];
	table_file_header *tfh;
	tfh = NULL;
	tfh = (table_file_header*)calloc(1, sizeof(table_file_header));

	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										tfh->record_size = tfh->record_size + 1 + sizeof(int);
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											tfh->record_size = tfh->record_size + 1 + atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						free(new_entry);
					}
				}
			}
		}
	}
	int remainder = tfh->record_size % 4;
	printf("Remainder: %d", remainder);
	printf("\n");
	if( remainder > 0) {
	 	tfh->record_size = tfh->record_size + (4 - remainder);
	}

	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;
	
	char table_name[20];
	strcpy(table_name, tab_entry.table_name);
	strcat(table_name, ".tab");
	//int *ptr;
	//ptr = (int*) malloc(100 * sizeof(int));

	printf("Record size: %d", tfh->record_size);
	printf("\n");
	tfh->file_size = sizeof(table_file_header);
	printf("File size: %d", tfh->file_size);
	printf("\n");
	tfh->num_records = 0;
	printf("Number of Records: %d", tfh->num_records);
	printf("\n");
	tfh->tpd_ptr = 0;
	printf("Ptr Value: %d", tfh->tpd_ptr);
	printf("\n");
	tfh->file_header_flag = 0;
	printf("Flag Value: %d", tfh->file_header_flag);
	printf("\n");
	tfh->record_offset = tfh->file_size;
	printf("Initial record offset: %d", tfh->record_offset);
	printf("\n");
  /* Open for read */
  if((fhandle = fopen(table_name, "rbc")) == NULL)
	{
		if((fhandle = fopen(table_name, "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			//tfh = NULL;
			//tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
			
			if (!tfh)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				tfh->file_size = sizeof(table_file_header);
				fwrite(tfh, sizeof(table_file_header), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	} else {

	}
    
	
  return rc;
}

int sem_insert_into_table(token_list *t_list){
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry *col_entry = NULL;
	int i;
	struct stat file_stat;
	table_file_header *new_header;
	table_file_header *old_header;
	char tablename[MAX_IDENT_LEN+1];
	int accum = 0;
	char *ch_ptr;
	//tfh = (table_file_header*)calloc(1, sizeof(table_file_header));cur = t_list;
	cur = t_list;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else {
			
			if(cur->next->tok_value != K_VALUES) {
				rc = INVALID_STATEMENT;
				cur->next->tok_value = INVALID;
			} else {
				cur = cur->next->next;
				if(cur->tok_value != S_LEFT_PAREN) {
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				} else {
					cur = cur->next;
					
					//int old_size = old_header -> file_size;
					
					//printf("Old size: %d", old_size);
					
					//table_file_header *old_header = (table_file_header *)malloc(sizeof(table_file_header));
					//memset(new_header, '\0', old_header->file_size);
					FILE *fhandle = NULL;
					char filename[MAX_IDENT_LEN + 4];
					

					strcpy(tablename, tab_entry->table_name);
					strcpy(filename, strcat(tab_entry->table_name, ".tab"));
					if ((fhandle = fopen(filename, "rbc")) == NULL)
					{
					printf("Error while opening %s file\n", filename);
					rc = FILE_OPEN_ERROR;
					cur->tok_value = INVALID;
					}
					else
					{
					fstat(fileno(fhandle), &file_stat);
					printf("Old header size = %d\n", file_stat.st_size);
					printf("\n");
					old_header = (table_file_header *)calloc(1, file_stat.st_size);
					// Read the old header information from the tab file.
					fread(old_header, file_stat.st_size, 1, fhandle);
					old_header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
					fclose(fhandle);
					
					int size = old_header->record_size;
					printf("Size: %d", size);
					printf("\n");
					printf("%d", old_header->file_size + size);
					printf("\n");
					new_header = (table_file_header *)calloc(1, old_header->file_size + size);
					//printf("IMP SIZE: %d", sizeof(new_header));
					//printf("\n");
					//rc = add_row_to_file(old_header, column_tokens);
					//Insert row here

					
					memcpy((void*)new_header,
							(void*)old_header,
							old_header->file_size);
					//new_header = new_header + old_header->file_size;
					/*******HERE********/
					new_header->num_records += 1;
					new_header->tpd_ptr = 0;
					new_header->file_size = old_header->file_size + size;

					//printf("NEW HEADER FILE SIZE: %d", new_header->file_size);
					//printf("\n");
					//accum += old_header->file_size;
					//new_header = new_header + old_header->file_size;
					//wr = wr + old_header->file_size;
					ch_ptr = (char*)new_header;
					ch_ptr += old_header->file_size;
					//new_header = new_header + *wr;

					if (rc == 0)
					{
					//printf("row inserted successfully.\n");
					//free(old_header);
					}
					else
					{
					//printf("Failure to insert.\n");
					}
					} 
					
					//printf("%d", cur->tok_value);
					for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							//printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							//printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							//printf("Current = %s\n", cur->tok_string);
							//cur = cur->next;
							if((col_entry->col_type == T_INT) && (cur->tok_value != INT_LITERAL)){
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							} 
							else if((col_entry->col_type == T_CHAR) && (cur->tok_value != STRING_LITERAL)){
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID; 
							} else {
								printf("Column Name   (col_name) = %s\n", col_entry->col_name);
								printf("Column Type   (col_type) = %d\n", col_entry->col_type);
								printf("\n");
								
								if(col_entry->col_type == T_INT){
									printf("IN INT: %d", atoi(cur->tok_string));
									printf("\n");
									int tmp_int = atoi(cur->tok_string);
									int len = 4;
									memcpy((void*)ch_ptr,
									(void*)&len,
									1);
									accum += 1;
									ch_ptr += 1;
									//new_header = new_header
									//accum += 1;
									//((int*)new_header +1);
									memcpy((void*)ch_ptr,
									(void*)&tmp_int,
									len);
									accum += len;
									ch_ptr += len;
									//new_header = new_header + len;
									//((int*)new_header +len);
									//accum += len;
									
								}
								if(col_entry->col_type == T_CHAR){
									printf(cur->tok_string);
									printf("\n");
									char* tmp_char = cur->tok_string;
									//printf("tmp: %s", tmp_char);
									//printf("\n");
									int len = col_entry->col_len;
									printf("LEN: %d", len);
									memcpy((void*)ch_ptr,
									(void*)&len,
									1);
									accum += 1 ;
									ch_ptr += 1;
									//new_header = new_header + 1;
									//((char*)new_header +1);
									memcpy((void*)ch_ptr,
									(void*)tmp_char,
									len);
									accum += len;
									ch_ptr += len;
									//printf("test: %s", *ch_ptr);
									//new_header = new_header + len;
									//((char*)new_header +len);
									//accum += len;
								}
								
							}
							cur = cur->next;
							if(cur->tok_value == S_COMMA){
								printf("COMMA\n");
								cur = cur->next;
							}

						}
						if(cur->tok_value != S_RIGHT_PAREN) {
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
						} else {
							strcat(tablename, ".tab");
							
							if((fhandle = fopen(tablename, "wbc")) == NULL)
							{
							rc = FILE_OPEN_ERROR;
							}
							else
							{
							//tfh = NULL;
							//tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
							
								if (!new_header)
								{
									rc = MEMORY_ERROR;
								}
								else
								{
									printf("ACCUM: %d", accum);
									printf("\n");
									
									//new_header->num_records += 1;
									//new_header->tpd_ptr = 0;
									//new_header->file_size = old_header->file_size +32;
									//new_header = new_header - (old_header->file_size + old_header->record_size);
									printf("NEW HEADER FILE SIZE: %d", new_header->file_size);
									printf("\n");
									printf("NUM RECORDS: %d", new_header->num_records);
									
									fwrite(new_header, new_header->file_size, 1, fhandle);
									//fseek(fhandle, 0, SEEK_SET );
									fflush(fhandle);
									fclose(fhandle);
									free(old_header);
									//free(ch_ptr);
								}
							}
							printf("\n");
							printf("INSERT Statement Parsed!\n");
						
							
						}
							
				}
			}
		}
	return rc;
	
}

void swap(char* arr1[], int i){
	char * temp_char = arr1[i];
	arr1[i] = arr1[i+1];
	arr1[i+1] = temp_char;
}
void swap_2D(char* arr1[100][16], int i, int j){
	char * temp_char = arr1[i][j];
	arr1[i][j] = arr1[i][j+1];
	arr1[i][j+1] = temp_char;
	

}


int sem_select(token_list *t_list){
	int rc = 0;
	token_list *cur;
	table_file_header* header;
	struct stat file_stat;
	tpd_entry *tab_entry = NULL;
	tpd_entry *tab_entry_two = NULL;
	cd_entry  *col_entry = NULL;
	cd_entry  *col_entry_two = NULL;
	cd_entry *col_entry_three = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	char tablename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	char *ch_ptr;
	char *ch_ptr_two;
	char *ch_ptr_print;
	int i = 0;
	int j = 0;
	int k = 0;
	char* colname;
	bool col_exists;
	int coltype;
	int collen;
	std::vector<char *> col_names;
	std::vector<int> col_types;
	std::vector<int> col_lens;
	std::vector<int> col_offsets;
	std::vector<bool> cols_exist;
	std::vector<int> col_nums;
	cur = t_list;
	int col_offset = 0;
	char* colname_two;
	char* colname_three;
	int col_type_two;
	int col_type_three;
	int col_len_two;
	int col_len_three;
	char* ch_val_cond;
	char* ch_val_cond_two;
	bool col_two_exists;
	bool col_three_exists;
	char* ch_ptr_read;
	char* ch_ptr_write;
	bool descending;
	int and_or;
	int op_one;
	if((cur->tok_value == K_SELECT) && (cur->next->tok_value != F_SUM) && (cur->next->tok_value != F_AVG) && (cur->next->tok_value != F_COUNT)){
		//cur = cur->next;

		// While cur->next not terminator, find colnames
		while(cur->tok_value != K_FROM){
			//printf("FINDING COLS\n");
			if((cur->tok_value != S_COMMA) && (cur->tok_value != K_SELECT)){
				col_names.push_back(cur->tok_string);
				printf("COLNAME: %s\n", cur->tok_string);
			}

			if(cur->tok_value == S_COMMA){
				if(cur->next->tok_value == K_FROM){
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				} 
				else {
					cur = cur->next;
				}
			} else {
				cur = cur->next;
			}
			 
		}
	 
		//colname = cur->tok_string;
		//cur = cur->next;
		printf("FROM: %s\n", cur->tok_string);
		if(cur->tok_value == K_FROM){
			cur = cur->next;
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			} else {
				for(j = 0; j < col_names.size(); j++){
					col_offset = 0;
					for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++) {
									col_offset += (col_entry->col_len + 1);
									if(strcmp(col_entry->col_name,col_names.at(j))==0){
										col_types.push_back(col_entry->col_type);
										col_lens.push_back(col_entry->col_len);
										cols_exist.push_back(true);
										col_offset -= (col_entry->col_len);
										col_offsets.push_back(col_offset);
										col_nums.push_back(i);
										//printf("NEW OFFSET: %d\n", col_offset);
										printf("COL NAME: %s\n", col_names.at(j));
										
									}
					}
				
				}
				if(cols_exist.size() == col_names.size()){
					col_exists = true;
					printf("COL EXISTS\n");
				} else {
					col_exists = false;
				}
				if(col_exists){
					if((cur->next->tok_class == terminator) 
					|| ((cur->next->tok_value == K_NATURAL) && (cur->next->next->tok_value == K_JOIN))
					|| (cur->next->tok_value == K_WHERE)
					|| ((cur->next->tok_value == K_ORDER) && (cur->next->next->tok_value == K_BY))){
						if(cur->next->tok_class == terminator){
							printf("SELECT colname FROM tablename STATEMENT\n");
							// Read in header
							FILE *fhandle = NULL;
							char filename[MAX_IDENT_LEN + 4];
							

							strcpy(tablename, tab_entry->table_name);
							strcpy(filename, strcat(tab_entry->table_name, ".tab"));
							if ((fhandle = fopen(filename, "rbc")) == NULL)
							{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
							}
							else
							{
							fstat(fileno(fhandle), &file_stat);
							printf("Header size = %d\n", file_stat.st_size);
							printf("\n");
							header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(header, file_stat.st_size, 1, fhandle);
							header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
							fclose(fhandle);
							}

							// Print colnames
							ch_ptr = (char*)header;
							ch_ptr += header->record_offset;
							int ctr = 0;
							int act_val = header->record_size;
							int remainder = 0;
							int i_;
							char c = '-';
							for(j = 0; j < col_names.size(); j++){
								for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++) {
									if((strcmp(col_entry->col_name, col_names.at(j)) == 0) 
										&& (col_entry->col_len == col_lens.at(j)) 
										&& (col_entry->col_type == col_types.at(j)))
										{
											if(col_entry->col_type == T_INT){
												printf("%16s", col_entry->col_name);
												printf("|");
											
											} else if(col_entry->col_type == T_CHAR){
												printf("%-16s", col_entry->col_name);
												printf("|");
											}
										}					

								}
							}
							
								
							printf("\n");
							for(int i_ = 0; i_ < 17 * j; i_++){
								putchar(c);
							}

							printf("\n");

							// Print the columns 
							//char* tbl_one[100][16];
							ch_ptr = (char*)header; 
							int total_offset;
							for(j = 0; j < header->num_records; j++){
								for(k = 0; k < col_names.size(); k++){
									ch_ptr = (char*)header;
									total_offset = (header->record_size * j) + col_offsets.at(k) + header->record_offset;
									ch_ptr += total_offset;
									if(col_types.at(k) == T_INT) {
										//printf("INT INSERTED\n");
										//tbl_one[j][k] = (char *) ch_ptr;
										printf("%16d", *(int*) ch_ptr);
										printf("|");
									}
									else if(col_types.at(k) == T_CHAR){
										char* tmp_char = (char*) malloc(col_lens.at(k) + 1);
										memset(tmp_char, '\0', col_lens.at(k) + 1);
										memcpy(tmp_char,
												ch_ptr,
												col_lens.at(k));
										printf("%-16s", tmp_char);
										printf("|");
									}
									
								}
								printf("\n");
							}
							printf("SELECT DONE\n");
						} else if(cur->next->tok_value == K_WHERE){
							printf("WHERE\n");
							cur = cur->next->next;
							printf("CUR: %s\n", cur->tok_string);
							for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry_two++)
								{
									if(strcmp(cur->tok_string, col_entry_two->col_name) == 0){
										col_two_exists = true;
										colname_two = cur->tok_string;
										printf("Col Name: %s\n", col_entry_two->col_name);
										col_type_two = col_entry_two->col_type;
									} 
								}
							if(col_two_exists){
								cur = cur->next;
								if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_GREATER) || (cur->tok_value == S_LESS)){
									op_one = cur->tok_value;
									ch_val_cond = cur->next->tok_string;
									printf("CHAR VAL COND: %s\n", ch_val_cond);
									// Read old header
									FILE *fhandle = NULL;
									char filename[MAX_IDENT_LEN + 4];
									

									strcpy(tablename, tab_entry->table_name);
									strcpy(filename, strcat(tab_entry->table_name, ".tab"));
									if ((fhandle = fopen(filename, "rbc")) == NULL)
									{
									printf("Error while opening %s file\n", filename);
									rc = FILE_OPEN_ERROR;
									cur->tok_value = INVALID;
									}
									else
									{
									fstat(fileno(fhandle), &file_stat);
									printf("Old header size = %d\n", file_stat.st_size);
									printf("\n");
									header = (table_file_header *)calloc(1, file_stat.st_size);
									fread(header, file_stat.st_size, 1, fhandle);
									header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
									fclose(fhandle);
									}
							

			
						
									if((cur->next->tok_class == terminator) || (cur->next->next->tok_value == EOC)){
									// Find matching rows
									std::vector<int> match_col_names;
									int num_rows_updated;

									ch_ptr = (char*)header;
									ch_ptr += header->record_offset;
									int ctr = 0;
									int act_val = header->record_size;
									int remainder = 0;

									for(j = 0; j < header->num_records; j++){
										for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, col_entry_two++) {
													if(col_entry_two->col_type == T_INT) {
														ch_ptr += 1;
														ctr += 1;

														int tmp_int = atoi(ch_val_cond);
													
														if(strcmp(col_entry_two->col_name,colname_two)==0){
															if(cur->tok_value == S_EQUAL){
																if(tmp_int == *(int*)ch_ptr){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_LESS){
																if(*(int*)ch_ptr < tmp_int){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_GREATER){
																if(*(int*)ch_ptr > tmp_int){
																	match_col_names.push_back(j);
															
																}
															}
														}
														
														
														ch_ptr += 4;
														ctr += 4;
													}
													else if(col_entry_two->col_type == T_CHAR) {
														ch_ptr += 1;
														ctr += 1;

														
														
														char* tmp_char = (char*) malloc(col_entry_two->col_len + 1);

														memset(tmp_char, '\0', col_entry_two->col_len + 1);
														memcpy(tmp_char,
																ch_ptr,
																col_entry_two->col_len);
														if(strcmp(col_entry_two->col_name,colname_two)==0){
															if(cur->tok_value == S_EQUAL){
																if(strcmp(ch_val_cond, tmp_char) == 0){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_LESS){
																if(strcmp(ch_val_cond, tmp_char) > 0){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_GREATER){
																if(strcmp(ch_val_cond, tmp_char) < 0){
																	match_col_names.push_back(j);
																}
															}
														}
														
														
														
														ch_ptr += col_entry_two->col_len;
														ctr += col_entry_two->col_len;
													}
													
										}
										if(j == 0){
											remainder =  act_val % ctr;
										}
												
										if(remainder != 0){
												ch_ptr += remainder;
										}
									
									}
									
									
									num_rows_updated = match_col_names.size();
									printf("Num rows selected: %d\n\n", num_rows_updated);
									if(num_rows_updated == 0){
										printf("WARNING: No rows selected.\n");
									}
									
									
									// Print column val for marked rows
									
									//ch_ptr_write += old_header->record_offset;
									ch_ptr_read = (char*)header; 
									//ch_ptr_read += old_header->record_offset;
									remainder = 0;
									int total_offset;
									// for(k = 0; k < col_names.size(); k++){
									// 	printf("COL NAME: %s\n", col_names.at(k));
									// }

									ctr = 0;
									act_val = header->record_size;
									int i_;
									char c = '-';
									for(j = 0; j < col_names.size(); j++){
										for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
										i < tab_entry->num_columns; i++, col_entry++) {
											if((strcmp(col_entry->col_name, col_names.at(j)) == 0) 
												&& (col_entry->col_len == col_lens.at(j)) 
												&& (col_entry->col_type == col_types.at(j)))
												{
													if(col_entry->col_type == T_INT){
														printf("%16s", col_entry->col_name);
														printf("|");
													
													} else if(col_entry->col_type == T_CHAR){
														printf("%-16s", col_entry->col_name);
														printf("|");
													}
												}					

										}
									}
									
										
									printf("\n");
									for(int i_ = 0; i_ < 17 * j; i_++){
										putchar(c);
									}

									printf("\n");
									
									bool print_n;
								
									for(j = 0; j < header->num_records; j++){
									//printf("j: %d\n", j);
										for(k = 0; k < num_rows_updated; k++){
											for(i = 0; i < col_names.size(); i++){
												if(match_col_names.at(k) == j){
												print_n = true;
												ch_ptr_read = (char*)header;
												total_offset = (header->record_size * j) + col_offsets.at(i) + header->record_offset;
												ch_ptr_read += total_offset;
												if(col_types.at(i) == T_INT){
													//int tmp_int = atoi(ch_val);

													printf("%16d", *(int*) ch_ptr_read);
													printf("|");

											
												} else if (col_types.at(i)  == T_CHAR){
													char* tmp_char = (char*) malloc(col_lens.at(i) + 1);
													memset(tmp_char, '\0', col_lens.at(i)+ 1);
													memcpy(tmp_char,
														ch_ptr_read,
														col_lens.at(i));

													printf("%-16s", tmp_char);
													printf("|");
												}
																								
												
												} else {
													print_n = false;
												}
												
											}
											if(print_n){
												printf("\n");
											}
											
											
										}
										
									}	

								  }
								  else if((cur->next->next->tok_value == K_ORDER) && (cur->next->next->next->tok_value == K_BY)){
								
									printf("SELECT {column_name} FROM table_name WHERE column_name <condition> ORDER BY column_name [DESC]\n");
								  }
								  else if((cur->next->next->tok_value == K_AND) || (cur->next->next->tok_value == K_OR)){
									  and_or = cur->next->next->tok_value;
									  cur = cur->next->next->next;;
									  
										printf("CUR: %s\n", cur->tok_string);
										for(i = 0, col_entry_three = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, col_entry_three++)
											{
												if(strcmp(cur->tok_string, col_entry_three->col_name) == 0){
													col_three_exists = true;
													colname_three = cur->tok_string;
													printf("Col Name: %s\n", col_entry_three->col_name);
													col_type_three = col_entry_three->col_type;
												} 
											}
										if(col_three_exists){
											printf("COL THREE EXISTS: %s\n", colname_three);
											cur = cur->next;
											if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_GREATER) || (cur->tok_value == S_LESS)){
												ch_val_cond_two = cur->next->tok_string;
												printf("CHAR VAL COND TWO: %s\n", ch_val_cond_two);
												// Find matching rows for cond 1
												std::vector<int> match_col_names;
												int num_rows_updated;

												ch_ptr = (char*)header;
												ch_ptr += header->record_offset;
												int ctr = 0;
												int act_val = header->record_size;
												int remainder = 0;

												for(j = 0; j < header->num_records; j++){
													for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
														i < tab_entry->num_columns; i++, col_entry_two++) {
																if(col_entry_two->col_type == T_INT) {
																	ch_ptr += 1;
																	ctr += 1;

																	int tmp_int = atoi(ch_val_cond);

																	if(strcmp(col_entry_two->col_name,colname_two)==0){
																		if(op_one == S_EQUAL){
																			if(tmp_int == *(int*)ch_ptr){
																				match_col_names.push_back(j);
																			}
																		} else if(op_one == S_LESS){
																			if(*(int*)ch_ptr < tmp_int){
																				match_col_names.push_back(j);
																			}
																		} else if(op_one == S_GREATER){
																			if(*(int*)ch_ptr > tmp_int){
																				match_col_names.push_back(j);
																		
																			}
																		}
																	}
																	
																	
																	ch_ptr += 4;
																	ctr += 4;
																}
																else if(col_entry_two->col_type == T_CHAR) {
																	ch_ptr += 1;
																	ctr += 1;

																	
																	
																	char* tmp_char = (char*) malloc(col_entry_two->col_len + 1);

																	memset(tmp_char, '\0', col_entry_two->col_len + 1);
																	memcpy(tmp_char,
																			ch_ptr,
																			col_entry_two->col_len);
																	if(strcmp(col_entry_two->col_name,colname_two)==0){
																		if(op_one == S_EQUAL){
																			if(strcmp(ch_val_cond, tmp_char) == 0){
																				match_col_names.push_back(j);
																			}
																		} else if(op_one == S_LESS){
																			if(strcmp(ch_val_cond, tmp_char) > 0){
																				match_col_names.push_back(j);
																			}
																		} else if(op_one == S_GREATER){
																			if(strcmp(ch_val_cond, tmp_char) < 0){
																				match_col_names.push_back(j);
																			}
																		}
																	}
																	
																	
																	
																	ch_ptr += col_entry_two->col_len;
																	ctr += col_entry_two->col_len;
																}
																
													}
													if(j == 0){
														remainder =  act_val % ctr;
													}
															
													if(remainder != 0){
															ch_ptr += remainder;
													}
												
												}
												
												
												num_rows_updated = match_col_names.size();
												printf("Num rows selected for first condition: %d\n", num_rows_updated);
												//---------------COND 2-------------------//
												// Find matching rows for cond 1
												std::vector<int> match_col_names_two;;
												int num_rows_updated_two;;

												ch_ptr = (char*)header;
												ch_ptr += header->record_offset;
												ctr = 0;
												act_val = header->record_size;
												remainder = 0;

												for(j = 0; j < header->num_records; j++){
													for(i = 0, col_entry_three = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
														i < tab_entry->num_columns; i++, col_entry_three++) {
																if(col_entry_three->col_type == T_INT) {
																	ch_ptr += 1;
																	ctr += 1;

																	int tmp_int = atoi(ch_val_cond_two);

																	if(strcmp(col_entry_three->col_name,colname_three)==0){
																		if(cur->tok_value == S_EQUAL){
																			if(tmp_int == *(int*)ch_ptr){
																				match_col_names_two.push_back(j);
																			}
																		} else if(cur->tok_value == S_LESS){
																			if(*(int*)ch_ptr < tmp_int){
																				match_col_names_two.push_back(j);
																			}
																		} else if(cur->tok_value == S_GREATER){
																			if(*(int*)ch_ptr > tmp_int){
																				match_col_names_two.push_back(j);
																		
																			}
																		}
																	}
																	
																	
																	ch_ptr += 4;
																	ctr += 4;
																}
																else if(col_entry_three->col_type == T_CHAR) {
																	ch_ptr += 1;
																	ctr += 1;

																	
																	
																	char* tmp_char = (char*) malloc(col_entry_three->col_len + 1);

																	memset(tmp_char, '\0', col_entry_three->col_len + 1);
																	memcpy(tmp_char,
																			ch_ptr,
																			col_entry_three->col_len);
																	if(strcmp(col_entry_three->col_name,colname_three)==0){
																		if(cur->tok_value == S_EQUAL){
																			if(strcmp(ch_val_cond_two, tmp_char) == 0){
																				match_col_names_two.push_back(j);
																			}
																		} else if(cur->tok_value == S_LESS){
																			if(strcmp(ch_val_cond_two, tmp_char) > 0){
																				match_col_names_two.push_back(j);
																			}
																		} else if(cur->tok_value == S_GREATER){
																			if(strcmp(ch_val_cond_two, tmp_char) < 0){
																				match_col_names_two.push_back(j);
																			}
																		}
																	}
																	
																	
																	
																	ch_ptr += col_entry_three->col_len;
																	ctr += col_entry_three->col_len;
																}
																
													}
													if(j == 0){
														remainder =  act_val % ctr;
													}
															
													if(remainder != 0){
															ch_ptr += remainder;
													}
												
												}
												
												
												num_rows_updated_two = match_col_names_two.size();
												printf("Num rows selected for second condition: %d\n", num_rows_updated_two);
												// LOOP to find common columns
												std::vector<int> match_col_names_combined;
												std::vector<int> match_col_names_;
												bool push;
												if(and_or == K_AND){
													for(i = 0; i < match_col_names.size(); i++){
														for(j = 0; j < match_col_names_two.size(); j++){
															if(match_col_names.at(i) == match_col_names_two.at(j)){
																match_col_names_combined.push_back(match_col_names.at(i));

															}
														}
													}
												} else if(and_or == K_OR){
													for(i = 0; i < match_col_names.size(); i++){
														for(j = 0; j < match_col_names_two.size(); j++){
															
															match_col_names_.push_back(match_col_names.at(i));
															match_col_names_.push_back(match_col_names_two.at(j));
														}
														
														
													}
													
													for(i = 0; i < match_col_names_.size(); i++) {
														for(j = i + 1; j < match_col_names_.size(); j++) {
															if(match_col_names_.at(i) == match_col_names_.at(j)) {
																match_col_names_combined.push_back(match_col_names_.at(i));
															} else {
																match_col_names_combined.push_back(match_col_names_.at(i));
																match_col_names_combined.push_back(match_col_names_.at(j));
															}
															
														}
													}
													
												}
												

												int num_rows_updated_final = match_col_names_combined.size();
												printf("Num rows selected: %d\n", num_rows_updated_final);
												// Print
												//ch_ptr_write += old_header->record_offset;
												ch_ptr_read = (char*)header; 
												//ch_ptr_read += old_header->record_offset;
												remainder = 0;
												int total_offset;
												// for(k = 0; k < col_names.size(); k++){
												// 	printf("COL NAME: %s\n", col_names.at(k));
												// }

												ctr = 0;
												act_val = header->record_size;
												int i_ = 0;;
												char c = '-';
												for(j = 0; j < col_names.size(); j++){
													for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
													i < tab_entry->num_columns; i++, col_entry++) {
														if((strcmp(col_entry->col_name, col_names.at(j)) == 0) 
															&& (col_entry->col_len == col_lens.at(j)) 
															&& (col_entry->col_type == col_types.at(j)))
															{
																if(col_entry->col_type == T_INT){
																	printf("%16s", col_entry->col_name);
																	printf("|");
																
																} else if(col_entry->col_type == T_CHAR){
																	printf("%-16s", col_entry->col_name);
																	printf("|");
																}
															}					

													}
												}
												
													
												printf("\n");
												for(int i_ = 0; i_ < 17 * j; i_++){
													putchar(c);
												}

												printf("\n");
												
												bool print_n;
											
												for(j = 0; j < header->num_records; j++){
												//printf("j: %d\n", j);
													for(k = 0; k < num_rows_updated_final; k++){
														for(i = 0; i < col_names.size(); i++){
															if(match_col_names_combined.at(k) == j){
															print_n = true;
															ch_ptr_read = (char*)header;
															total_offset = (header->record_size * j) + col_offsets.at(i) + header->record_offset;
															ch_ptr_read += total_offset;
															if(col_types.at(i) == T_INT){
																//int tmp_int = atoi(ch_val);

																printf("%16d", *(int*) ch_ptr_read);
																printf("|");

														
															} else if (col_types.at(i)  == T_CHAR){
																char* tmp_char = (char*) malloc(col_lens.at(i) + 1);
																memset(tmp_char, '\0', col_lens.at(i)+ 1);
																memcpy(tmp_char,
																	ch_ptr_read,
																	col_lens.at(i));

																printf("%-16s", tmp_char);
																printf("|");
															}
																											
															
															} else {
																print_n = false;
															}
															
														}
														if(print_n){
															printf("\n");
														}
														
														
													}
													
												}	

											}
										}

								 
								 } 
								  else {
									rc = INVALID_STATEMENT;
									cur->next->next->tok_value = INVALID;
								  }
									
								}
							}
								
							 
						}
						else if((cur->next->tok_value == K_ORDER) && (cur->next->next->tok_value == K_BY)){
							printf("ORDER BY\n");
							cur = cur->next->next->next;
							printf("CUR: %s\n", cur->tok_string);
							for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry_two++)
								{
									if(strcmp(cur->tok_string, col_entry_two->col_name) == 0){
										col_two_exists = true;
										colname_two = cur->tok_string;
										printf("Col Name: %s\n", col_entry_two->col_name);
										col_type_two = col_entry_two->col_type;
										printf("COL TYPE: %d\n", col_type_two);
									} 
								}
							if(col_two_exists){
								printf("COL 2 exists\n");
								if(cur->next->tok_class != terminator){
									if(cur->next->tok_value == K_DESC){
										descending = true;
										printf("ORDER BY DESCENDING\n");
									} else {
										rc = INVALID_STATEMENT;
										cur->next->tok_value = INVALID;
									}
								}
								// Read old header
								FILE *fhandle = NULL;
								char filename[MAX_IDENT_LEN + 4];
								

								strcpy(tablename, tab_entry->table_name);
								strcpy(filename, strcat(tab_entry->table_name, ".tab"));
								if ((fhandle = fopen(filename, "rbc")) == NULL)
								{
								printf("Error while opening %s file\n", filename);
								rc = FILE_OPEN_ERROR;
								cur->tok_value = INVALID;
								}
								else
								{
								fstat(fileno(fhandle), &file_stat);
								printf("Old header size = %d\n", file_stat.st_size);
								printf("\n");
								header = (table_file_header *)calloc(1, file_stat.st_size);
								fread(header, file_stat.st_size, 1, fhandle);
								header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
								fclose(fhandle);
								}

								// Loop through table one and store data in array
								char* tbl_one[100][16];
								char* col_vals[100];
								ch_ptr = (char*)header;
								ch_ptr += header->record_offset;
								int ctr = 0;
								int act_val = header->record_size;
								int remainder = 0;
								
								for(j = 0; j < header->num_records; j++){
									for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
										i < tab_entry->num_columns; i++, col_entry++) {
												if(col_entry->col_type == T_INT) {
													ch_ptr += 1;
													ctr += 1;

													//printf("%16d", *(int*) ch_ptr);
													//printf("|");
													if(strcmp(col_entry->col_name, colname_two) == 0){
														col_vals[j] = (char *) ch_ptr;
													}
													if(strcmp)
													tbl_one[j][i] = (char *) ch_ptr;
													//printf("%16d", *(int*) tbl_one[j][i]);
													//printf("|");
													ch_ptr += 4;
													ctr += 4;
												}
												else if(col_entry->col_type == T_CHAR) {
													ch_ptr += 1;
													ctr += 1;
											
													char* tmp_char = (char*) malloc(col_entry->col_len + 1);
													memset(tmp_char, '\0', col_entry->col_len + 1);
													memcpy(tmp_char,
															ch_ptr,
															col_entry->col_len);
													//printf("%-16s", tmp_char);
													tbl_one[j][i] = tmp_char;
													if(strcmp(col_entry->col_name, colname_two) == 0){
														col_vals[j] = tmp_char;
													}
													//printf("%-16s", tbl_one[j][i]);
													
													ch_ptr += col_entry->col_len;
													ctr += col_entry->col_len;
												}
												
									}
									if(j == 0){
										remainder =  act_val % ctr;
									}
											
									if(remainder != 0){
											ch_ptr += remainder;
									}
								
								}
								//int temp;
								char* temp_char;
								int ROW = header->num_records;
								int COL = 0;
								int COL_NUM = 0;
								for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
										
										if(strcmp(col_entry->col_name, colname_two) == 0){
											COL_NUM = COL;
										}
										COL += 1;
								}
								
								//printf("COL NUM: %d\n", COL_NUM);

								for (i = 0; i < ROW; i++) {
									for (j = i+1; j < ROW; j++) {
										if(!descending){
											if(col_type_two == T_INT){
												if(*(int*)tbl_one[i][COL_NUM] > *(int*)tbl_one[j][COL_NUM]){
													for(int k = 0; k < COL; k++){
														char* temp_two = tbl_one[i][k];
														tbl_one[i][k] = tbl_one[j][k];
														tbl_one[j][k] = temp_two;
													}
												}
											} else if(col_type_two == T_CHAR){
												if(strcmp(tbl_one[i][COL_NUM], tbl_one[j][COL_NUM]) > 0){
													for(int k = 0; k < COL; k++){
														char* temp_two = tbl_one[i][k];
														tbl_one[i][k] = tbl_one[j][k];
														tbl_one[j][k] = temp_two;
													}
												}
											}
											
										} else if(descending){
											if(col_type_two == T_INT){
												if(*(int*)tbl_one[i][COL_NUM] < *(int*)tbl_one[j][COL_NUM]){
													for(int k = 0; k < COL; k++){
														char* temp_two = tbl_one[i][k];
														tbl_one[i][k] = tbl_one[j][k];
														tbl_one[j][k] = temp_two;
													}
												}
											} else if(col_type_two == T_CHAR){
												if(strcmp(tbl_one[i][COL_NUM], tbl_one[j][COL_NUM]) < 0){
													for(int k = 0; k < COL; k++){
														char* temp_two = tbl_one[i][k];
														tbl_one[i][k] = tbl_one[j][k];
														tbl_one[j][k] = temp_two;
													}
												}
											}
										}
									}
								}
								

								// Print colnames
								int i_;
								char c = '-';
								for(j = 0; j < col_names.size(); j++){
									for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
										if((strcmp(col_entry->col_name, col_names.at(j)) == 0) 
											&& (col_entry->col_len == col_lens.at(j)) 
											&& (col_entry->col_type == col_types.at(j)))
											{
												if(col_entry->col_type == T_INT){
													printf("%16s", col_entry->col_name);
													printf("|");
												
												} else if(col_entry->col_type == T_CHAR){
													printf("%-16s", col_entry->col_name);
													printf("|");
												}
											}					

									}
								}
								
									
								printf("\n");
								for(int i_ = 0; i_ < 17 * j; i_++){
									putchar(c);
								}

								printf("\n");
								
								
								for(j = 0; j < header->num_records; j++){
									for(k = 0; k < col_names.size(); k++){
										// ch_ptr = (char*)header;
										// total_offset = (header->record_size * j) + col_offsets.at(k) + header->record_offset;
										// ch_ptr += total_offset;
										if(col_types.at(k) == T_INT) {
											//printf("INT INSERTED\n");
											//tbl_one[j][k] = (char *) ch_ptr;
											printf("%16d", *(int*) tbl_one[j][col_nums.at(k)]);
											printf("|");
										}
										else if(col_types.at(k) == T_CHAR){
											char* tmp_char = (char*) malloc(col_lens.at(k) + 1);
											memset(tmp_char, '\0', col_lens.at(k) + 1);
											memcpy(tmp_char,
													ch_ptr,
													col_lens.at(k));
											printf("%-16s", tbl_one[j][col_nums.at(k)]);
											printf("|");
										}
										
									}
									printf("\n");
								}
								
 
									
							
							}
							printf("\n");
							if((cur->next->tok_class == terminator) || ((cur->next->tok_value == K_DESC) && (cur->next->next->tok_value == EOC))) {
								printf("SELECT {column_name} FROM table_name ORDER BY column_name [DESC]!\n");

							} else {
								rc = INVALID_STATEMENT;
								cur->next->next->tok_value = INVALID;
							}
						}
						
						
							
						
					} else {
						rc = INVALID_STATEMENT;
						cur->next->tok_value = INVALID;
					}
				} else {
					rc = INVALID_COLUMN_NAME;
					cur->tok_value = INVALID;
				}
			}
			
		} else {
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		
	}
	else if((cur->next->tok_value == F_SUM) || (cur->next->tok_value == F_AVG) || (cur->next->tok_value == F_COUNT)){
		printf("AGGREGATE\n");
		printf(cur->tok_string);
		int aggregate;
		int cnt;
		int avg;
		int sum;
		int coloff;
		int colnum;
		int agg_col_type;
		if(cur->next->tok_value == F_SUM){
			printf("SUM\n");
			aggregate = F_SUM;
		} else if(cur->next->tok_value == F_AVG){
			printf("AVG\n");
			aggregate = F_AVG;
		} 
		
		cur = cur->next->next;
		if(cur->tok_value != S_LEFT_PAREN){
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		} else {
			cur = cur->next;
			colname = cur->tok_string;
			printf("COL NAME: %s\n", colname);
			cur = cur->next;
			if(cur->tok_value != S_RIGHT_PAREN){
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
			} else {
				cur = cur->next;
			}
		}
	 
		//colname = cur->tok_string;
		//cur = cur->next;
		printf("FROM: %s\n", cur->tok_string);
		if(cur->tok_value == K_FROM){
			cur = cur->next;
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			} else {
				
				col_offset = 0;
				for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++) {
								col_offset += (col_entry->col_len + 1);
								if(strcmp(col_entry->col_name,colname)==0){
									agg_col_type = col_entry->col_type;
									collen = col_entry->col_len;
									col_exists = true;
								
									col_offset -= (col_entry->col_len);
									
									colnum = i;
									printf("NEW OFFSET: %d\n", col_offset);
									printf("COL NAME: %s\n", colname);
									break;
									
								}
				}
				if((aggregate == F_SUM) || (aggregate == F_AVG)){
					if(agg_col_type != T_INT){
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						printf("You can only perform SUM and AVG on INT columns.\n");
						col_exists = false;
					}
				}
				
				if(col_exists){
					
					printf("COL EXISTS\n");
				} else {
					col_exists = false;
				}
				if(col_exists){
					if((cur->next->tok_class == terminator) 
					|| ((cur->next->tok_value == K_NATURAL) && (cur->next->next->tok_value == K_JOIN))
					|| (cur->next->tok_value == K_WHERE)
					|| ((cur->next->tok_value == K_ORDER) && (cur->next->next->tok_value == K_BY))){
						if(cur->next->tok_class == terminator){
							
							printf("SELECT aggregate(colname) FROM tablename STATEMENT\n");
							// Read in header
							FILE *fhandle = NULL;
							char filename[MAX_IDENT_LEN + 4];
							

							strcpy(tablename, tab_entry->table_name);
							strcpy(filename, strcat(tab_entry->table_name, ".tab"));
							if ((fhandle = fopen(filename, "rbc")) == NULL)
							{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
							}
							else
							{
							fstat(fileno(fhandle), &file_stat);
							printf("Header size = %d\n", file_stat.st_size);
							printf("\n");
							header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(header, file_stat.st_size, 1, fhandle);
							header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
							fclose(fhandle);
							}

							// Print colnames
							ch_ptr = (char*)header;
							ch_ptr += header->record_offset;
							int ctr = 0;
							int act_val = header->record_size;
							int remainder = 0;
							int i_;
							char c = '-';
							
							for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++) {
								if((strcmp(col_entry->col_name, colname) == 0) 
									&& (col_entry->col_len == collen) 
									&& (col_entry->col_type == agg_col_type))
									{
										if(aggregate == F_SUM){
											printf("(SUM) ");
											printf("%16s", col_entry->col_name);
											printf("|");
										} else if (aggregate == F_AVG){
											printf("(AVG) ");
											printf("%16s", col_entry->col_name);
											printf("|");
										}	
										
									}					

							}
							
							
								
							printf("\n");
							for(int i_ = 0; i_ < 23; i_++){
								putchar(c);
							}

							printf("\n");

							// Print the columns 
							//char* tbl_one[100][16];
							ch_ptr = (char*)header; 
							int total_offset;
							for(j = 0; j < header->num_records; j++){
								
								ch_ptr = (char*)header;
								total_offset = (header->record_size * j) + col_offset + header->record_offset;
								ch_ptr += total_offset;
								sum += *(int*) ch_ptr;
								
	
							}
							avg = sum/header->num_records;
							if(aggregate == F_SUM){
								printf("%d\n\n", sum);
							} else if(aggregate == F_AVG){
								printf("%d\n\n", avg);
							}
							
							printf("SELECT AGGREGATE DONE\n");
						} 
						
						
						
							
						
					} else {
						rc = INVALID_STATEMENT;
						cur->next->tok_value = INVALID;
					}
				} else {
					rc = INVALID_COLUMN_NAME;
					cur->tok_value = INVALID;
				}
			}
			
		} else {
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
	}
	else if(cur->tok_value != K_SELECT){
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		} else {
			printf("\n");
			printf("TABLE EXISTS");
			printf("\n");

			// Read data into a header
			FILE *fhandle = NULL;
			char filename[MAX_IDENT_LEN + 4];
			

			strcpy(tablename, tab_entry->table_name);
			strcpy(filename, strcat(tab_entry->table_name, ".tab"));
			if ((fhandle = fopen(filename, "rbc")) == NULL)
			{
			printf("Error while opening %s file\n", filename);
			rc = FILE_OPEN_ERROR;
			cur->tok_value = INVALID;
			}
			else
			{
			fstat(fileno(fhandle), &file_stat);
			printf("Header size = %d\n", file_stat.st_size);
			printf("\n");
			header = (table_file_header *)calloc(1, file_stat.st_size);
			fread(header, file_stat.st_size, 1, fhandle);
			header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
			fclose(fhandle);
			}
			
			if (cur->next->tok_value != K_NATURAL){
				// Make char * ptr and initially set to header + offset
				ch_ptr = (char*)header;
				ch_ptr += header->record_offset;
				int ctr = 0;
				int act_val = header->record_size;
				int remainder = 0;
				int i_;
				char c = '-';

				for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
										if(col_entry->col_type == T_INT){
											printf("%16s", col_entry->col_name);
											printf("|");
											
										} else if(col_entry->col_type == T_CHAR){
											printf("%-16s", col_entry->col_name);
											printf("|");
										}
										

									}
					
				printf("\n");
				for(int i_ = 0; i_ < 17 * i; i_++){
					putchar(c);
				}
				for(j = 0; j < header->num_records; j++){
					//printf("Num records: %d", header->num_records);
					printf("\n");
					for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
										
										//printf("%s\n", col_entry->col_name);
										
										
										if(col_entry->col_type == T_INT) {
												//printf ("%d", *(int*) ch_ptr);
												//printf(col_entry->col_name);
												ch_ptr += 1;
												ctr += 1;
												printf("%16d", *(int*) ch_ptr);
												printf("|");
												//printf("     ");
												ch_ptr += 4;
												ctr += 4;
											}
											else if(col_entry->col_type == T_CHAR) {
												ch_ptr += 1;
												ctr += 1;
												//printf("COL LEN: %d\n", col_entry->col_len + 1);
												char* tmp_char = (char*) malloc(col_entry->col_len + 1);
												memset(tmp_char, '\0', col_entry->col_len + 1);
												memcpy(tmp_char,
														ch_ptr,
														col_entry->col_len);
												printf("%-16s", tmp_char);
												printf("|");
												//printf("     ");
												
												
												ch_ptr += col_entry->col_len;
												ctr += col_entry->col_len;
											}
									}
									//printf("%d", ctr);
									//printf("\n");
									// printf("\n");
									// act_val += header->record_size;
									// printf("%d", act_val);
									// printf("\n");
									// int remainder =  act_val % ctr;
									// printf("remainder: %d", remainder);
									// printf("\n");
									if(j == 0){
										remainder =  act_val % ctr;
									}
									
									if(remainder != 0){
										ch_ptr += remainder;
									}
											
								}
			} else if(cur->next->tok_value == K_NATURAL){
				cur = cur->next->next;
				if(cur->tok_value == K_JOIN){
					cur = cur->next;
					if ((tab_entry_two = get_tpd_from_list(cur->tok_string)) == NULL)
						{
							rc = TABLE_NOT_EXIST;
							cur->tok_value = INVALID;
							printf("That table does not exist.");
							printf("\n");
						} else {
							// NATURAL JOIN BEGINS HERE
							printf("NATURAL JOIN Statement");
							printf("\n");

							// Read table 2 into TFH
							table_file_header* header_two;
							FILE *fhandle_two = NULL;
							char filename_two[MAX_IDENT_LEN + 4];
							char tablename_two[MAX_IDENT_LEN+1];

							strcpy(tablename_two, tab_entry_two->table_name);
							strcpy(filename_two, strcat(tab_entry_two->table_name, ".tab"));
							if ((fhandle_two = fopen(filename_two, "rbc")) == NULL)
							{
							printf("Error while opening %s file\n", filename_two);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
							}
							else
							{
							fstat(fileno(fhandle_two), &file_stat);
							printf("Header size = %d\n", file_stat.st_size);
							printf("\n");
							header_two = (table_file_header *)calloc(1, file_stat.st_size);
							fread(header_two, file_stat.st_size, 1, fhandle_two);
							header_two->tpd_ptr = get_tpd_from_list(tab_entry_two->table_name);
							fclose(fhandle_two);
							}
							printf("Header One: %d\n", header->record_size);
							printf("Header Two: %d\n", header_two->record_size);

							std::vector<char *> match_col_names;
							// Loop through table one and table two to check for matching name, type, and length
							for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
										for(k = 0, col_entry_two = (cd_entry*)((char*)tab_entry_two + tab_entry_two->cd_offset);
												k < tab_entry_two->num_columns; k++, col_entry_two++) {
													if(strcmp(col_entry->col_name,col_entry_two->col_name) == 0
														&& (col_entry->col_type == col_entry_two->col_type)
														&& (col_entry->col_len == col_entry_two->col_len)) {
														printf("MATCH\n");
														match_col_names.push_back(col_entry->col_name);
													}
													
													
									}
							}
							
							// Loop through table one and store data in array
							char* tbl_one[100][16];
							ch_ptr = (char*)header;
							ch_ptr += header->record_offset;
							int ctr = 0;
							int act_val = header->record_size;
							int remainder = 0;
							for(j = 0; j < header->num_records; j++){
								for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
											if(col_entry->col_type == T_INT) {
												ch_ptr += 1;
												ctr += 1;

												//printf("%16d", *(int*) ch_ptr);
												//printf("|");

												tbl_one[j][i] = (char *) ch_ptr;
												//printf("%16d", *(int*) tbl_one[j][i]);
												//printf("|");
												ch_ptr += 4;
												ctr += 4;
											}
											else if(col_entry->col_type == T_CHAR) {
												ch_ptr += 1;
												ctr += 1;
										
												char* tmp_char = (char*) malloc(col_entry->col_len + 1);
												memset(tmp_char, '\0', col_entry->col_len + 1);
												memcpy(tmp_char,
														ch_ptr,
														col_entry->col_len);
												//printf("%-16s", tmp_char);
												tbl_one[j][i] = tmp_char;
												//printf("%-16s", tbl_one[j][i]);
												
												ch_ptr += col_entry->col_len;
												ctr += col_entry->col_len;
											}
											
								}
								if(j == 0){
									remainder =  act_val % ctr;
								}
										
								if(remainder != 0){
										ch_ptr += remainder;
								}
							
							}

							// Loop through table two and store data in array
							char* tbl_two[100][16];
							ch_ptr_two = (char*)header_two;
							ch_ptr_two += header_two->record_offset;
							int ctr_two = 0;
							int act_val_two = header_two->record_size;
							int remainder_two = 0;
							for(j = 0; j < header_two->num_records; j++){
								for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry_two + tab_entry_two->cd_offset);
									i < tab_entry_two->num_columns; i++, col_entry_two++) {
											if(col_entry_two->col_type == T_INT) {
												ch_ptr_two += 1;
												ctr_two += 1;

												//printf("%16d", *(int*) ch_ptr);
												//printf("|");

												tbl_two[j][i] = (char *) ch_ptr_two;
												//printf("%16d", *(int*) tbl_two[j][i]);
												//printf("|");
												ch_ptr_two += 4;
												ctr_two += 4;
											}
											else if(col_entry_two->col_type == T_CHAR) {
												ch_ptr_two += 1;
												ctr_two += 1;
										
												char* tmp_char_two = (char*) malloc(col_entry_two->col_len + 1);
												memset(tmp_char_two, '\0', col_entry_two->col_len + 1);
												memcpy(tmp_char_two,
														ch_ptr_two,
														col_entry_two->col_len);
												//printf("%-16s", tmp_char);
												tbl_two[j][i] = tmp_char_two;
												//printf("%-16s", tbl_two[j][i]);
												
												ch_ptr_two += col_entry_two->col_len;
												ctr_two += col_entry_two->col_len;
											}
											
								}
								if(j == 0){
									remainder_two =  act_val_two % ctr_two;
								}
										
								if(remainder_two != 0){
										ch_ptr_two += remainder_two;
								}
							
							}
							int cnt_ = 0;
							//Print Column Names for first table
							for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
										if(col_entry->col_type == T_INT){
											printf("%16s", col_entry->col_name);
											printf("|");
											
										} else if(col_entry->col_type == T_CHAR){
											printf("%-16s", col_entry->col_name);
											printf("|");
										}
										cnt_ += 1;

									}
							char c = '-';
							int z;
							// for(z = 0; z < match_col_names.size(); z++){
							// 	printf("Match Col: %s\n", match_col_names.at(z));
							// }
		
							
							//Print Column Names for second table (non-matching columns)
							for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry_two + tab_entry_two->cd_offset);
									i < tab_entry_two->num_columns; i++, col_entry_two++) {
										// if(i > j+1){
										// 	break;
										// }
										
									//printf("Match COL(i): %s\n", match_col_names.at(i));
									//printf("Match COL(j): %s\n", match_col_names.at(j));
										if(match_col_names.size() > 0){
											if(strcmp(match_col_names.at(0), col_entry_two->col_name) != 0){
											//printf("NOT MATCH COL: %s\n", col_entry_two->col_name);
											if(col_entry_two->col_type == T_INT){
											printf("%16s", col_entry_two->col_name);
											printf("|");
										
											} else if(col_entry_two->col_type == T_CHAR){
											printf("%-16s", col_entry_two->col_name);
											printf("|");
											}
											cnt_ += 1;
											
										}
									}

									
											
										
										

								}
							printf("\n");
							for(int i_ = 0; i_ < 17 * cnt_; i_++){
								putchar(c);
							}
							printf("\n");
							int k;
							int x;
							int v;
							int f;
							int d;
							bool check_match;
							
							//tpd_entry *tab_entry_print = NULL;
							//cd_entry  *col_entry_print = NULL;
							for (i = 0; i < header->num_records; i++){
								for(j = 0; j < header_two->num_records; j++){
									for(k = 0; k < match_col_names.size(); k++){
										char* tmp_var = match_col_names.at(k);
										for(x = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
										x < tab_entry->num_columns; x++, col_entry++) {
											if(strcmp(tmp_var, col_entry->col_name) == 0){
												for(v = 0, col_entry_two = (cd_entry*)((char*)tab_entry_two + tab_entry_two->cd_offset);
													v < tab_entry_two->num_columns; v++, col_entry_two++) {
														if(strcmp(tmp_var, col_entry_two->col_name) == 0){
															if(strcmp(tbl_one[i][x], tbl_two[j][v]) == 0){
																check_match = true;
																
																if(check_match){	
																	for(x = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
																		x < tab_entry->num_columns; x++, col_entry++) {
																			if(col_entry->col_type == T_INT) {
																				
																				printf("%16d", *(int*) tbl_one[i][x]);
																				printf("|");
																
																			}
																			else if(col_entry->col_type == T_CHAR) {
																		
																				printf("%-16s", tbl_one[i][x]);
																				printf("|");
																	
																		
																		
																			}
																		
																	}
																	for(v = 0, col_entry_two = (cd_entry*)((char*)tab_entry_two + tab_entry_two->cd_offset);
																			v < tab_entry_two->num_columns; v++, col_entry_two++) {
																			if(match_col_names.size() > 0){
																				if(strcmp(match_col_names.at(0), col_entry_two->col_name) != 0){
																					if(col_entry_two->col_type == T_INT) {
																				
																						printf("%16d", *(int*) tbl_two[j][v]);
																						printf("|");
																
																					}
																					else if(col_entry_two->col_type == T_CHAR) {
																		
																						printf("%-16s", tbl_two[j][v]);
																						printf("|");
																	
																		
																		
																					}	
																				}	
																			}
																
																						
																			
																	}
																	printf("\n");
																}
														}
													}
												}
											}
										}
									}
									
							  }
								
									
							}
						// printf("\n");
						// printf("%16d", *(int*)tbl_one[0][0]);
						// printf("\n");
						// printf("\n");
						// printf("%s", tbl_one[0][1]);
						// printf("\n");
						// for(j = 0; j < 100; j++){
						// 	for(i = 0; i < 16; i++){
						// 		printf("\n");
								
						// 		printf("%s", tbl_one[j][i]);
						// 		printf("\n");
						// 	}
						// }
								
				}
				
				
			} else {
				rc = INVALID_STATEMENT;
				cur->next->tok_value = INVALID;
			}
			
			


		}
		

		// Loop through the columns
		   // Loop through the rows
		      // Int -> print with %d (int) ptr
			  // Char -> print with %s ptr
			  // Figure out how to format the printing
	//std::vector<char*> g1;
	}
	}
	
	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				remove(strcat(cur->tok_string, ".tab"));
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int sem_delete(token_list *t_list){
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry *col_entry = NULL;
	int i;
	int j;
	int k;
	struct stat file_stat;
	table_file_header *new_header;
	table_file_header *old_header;
	char tablename[MAX_IDENT_LEN+1];
	char *colname;
	int accum = 0;
	char *ch_ptr;
	bool col_exists;
	int col_type;
	char *ch_val;
	//bool skip_row;
	bool skip_two;
	bool write_row;
	//tfh = (table_file_header*)calloc(1, sizeof(table_file_header));cur = t_list;
	cur = t_list;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) 
	{
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
	}
	else 
	{
		printf("TABLE EXISTS\n");
		if((cur->next->tok_value == K_WHERE)) {
			printf("WHERE CONDITIONAL\n");
			cur = cur->next->next;
			for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
				{
					//printf("col: %s\n", col_entry->col_name);
					if(strcmp(cur->tok_string, col_entry->col_name) == 0){
						col_exists = true;
						colname = cur->tok_string;
						col_type = cur->tok_value;
					}
				}
			if(col_exists){
				printf("COL EXISTS: %s\n", cur->tok_string);
				cur = cur->next;
				printf("Cur: %d\n", cur->tok_value);
				if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_GREATER) || (cur->tok_value == S_LESS)) {
					printf("IN %s\n", cur->tok_string);


					ch_val = cur->next->tok_string;
					printf("CHAR VAL: %s\n", ch_val);
					// Open file for read
					FILE *fhandle = NULL;
					char filename[MAX_IDENT_LEN + 4];

					strcpy(tablename, tab_entry->table_name);
					strcpy(filename, strcat(tab_entry->table_name, ".tab"));
					if ((fhandle = fopen(filename, "rbc")) == NULL)
					{
					printf("Error while opening %s file\n", filename);
					rc = FILE_OPEN_ERROR;
					cur->tok_value = INVALID;
					}
					else
					{
					fstat(fileno(fhandle), &file_stat);
					printf("Old header size = %d\n", file_stat.st_size);
					printf("\n");
					old_header = (table_file_header *)calloc(1, file_stat.st_size);
					// Read the old header information from the tab file.
					fread(old_header, file_stat.st_size, 1, fhandle);
					old_header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
					fclose(fhandle);
					}



					std::vector<int> match_col_names;
					int num_rows_deleted;

					char* tbl_one[100][16];
					ch_ptr = (char*)old_header;
					ch_ptr += old_header->record_offset;
					int ctr = 0;
					int act_val = old_header->record_size;
					int remainder = 0;
					for(j = 0; j < old_header->num_records; j++){
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++) {
									if(col_entry->col_type == T_INT) {
										ch_ptr += 1;
										ctr += 1;



										//tbl_one[j][i] = (char *) ch_ptr;
										int tmp_int = atoi(ch_val);
										//printf("CH VAL INT: %s\n", ch_val);
										//printf("INT VAL: %d\n", (int*)ch_ptr);
										if(strcmp(col_entry->col_name,colname)==0){
											if(cur->tok_value == S_EQUAL){
												if(tmp_int == *(int*)ch_ptr){
													//mark equal row nums
													match_col_names.push_back(j);
													//printf("ROW NUM: %d\n", j);
													//printf("GENDER: %s\n", ch_ptr);
													//printf("IN MATCH COL NAMES LOOP INT\n");
												}
											} else if(cur->tok_value == S_LESS){
												if(*(int*)ch_ptr < tmp_int){
													//printf("LESS THAN ROWS\n");
													//mark row nums
													match_col_names.push_back(j);
													//printf("ROW VAL: %d\n", *(int*)ch_ptr);
											
												}
											} else if(cur->tok_value == S_GREATER){
												if(*(int*)ch_ptr > tmp_int){
													//mark row nums
													match_col_names.push_back(j);
											
												}
											}
										}
										
										
										ch_ptr += 4;
										ctr += 4;
									}
									else if(col_entry->col_type == T_CHAR) {
										ch_ptr += 1;
										ctr += 1;

										
										
										char* tmp_char = (char*) malloc(col_entry->col_len + 1);

										memset(tmp_char, '\0', col_entry->col_len + 1);
										memcpy(tmp_char,
												ch_ptr,
												col_entry->col_len);
										if(strcmp(col_entry->col_name,colname)==0){
											if(cur->tok_value == S_EQUAL){
												if(strcmp(ch_val, tmp_char) == 0){
													//mark row nums
													match_col_names.push_back(j);
													//printf("ROW NUM: %d\n", j);
													//printf("GENDER: %s\n", tmp_char);
													//printf("IN MATCH COL NAMES LOOP 2\n");
												}
											} else if(cur->tok_value == S_LESS){
												if(strcmp(ch_val, tmp_char) > 0){
													//mark row nums
													match_col_names.push_back(j);
												}
											} else if(cur->tok_value == S_GREATER){
												if(strcmp(ch_val, tmp_char) < 0){
													//mark row nums
													match_col_names.push_back(j);
												}
											}
										}
										
										
										
										ch_ptr += col_entry->col_len;
										ctr += col_entry->col_len;
									}
									
						}
						if(j == 0){
							remainder =  act_val % ctr;
						}
								
						if(remainder != 0){
								ch_ptr += remainder;
						}
					
					}
					
					
					num_rows_deleted = match_col_names.size();
					printf("Num rows deleted: %d\n", num_rows_deleted);
					if(num_rows_deleted == 0){
						printf("WARNING: No rows deleted.\n");
					}
					
					int size = old_header->record_size;
					
					new_header = (table_file_header *)calloc(1, old_header->file_size - (num_rows_deleted * old_header->record_size));
					
					
					memcpy((void*)new_header,
							(void*)old_header,
							old_header->file_size - (num_rows_deleted * old_header->record_size));
			
					new_header->num_records = old_header->num_records - num_rows_deleted;
					new_header->tpd_ptr = 0;
					new_header->file_size = old_header->file_size - (num_rows_deleted * old_header->record_size);

	
					char * ch_ptr_write = (char*)new_header;
					ch_ptr_write += old_header->record_offset;
					char * ch_ptr_read = (char*)old_header; 
					ch_ptr_read += old_header->record_offset;
				
					
					ctr = 0;
					act_val = old_header->record_size;
					remainder = 0;

					
					cur = cur->next;
					for(j = 0; j < old_header->num_records; j++){
						for(k = 0; k < match_col_names.size(); k++){
							if(match_col_names.at(k) != j){
								//printf("NON MATCH COL %d\n: ", j);
								write_row = true;
							}  else {
								write_row = false;
								break;
							}
						}
						if(write_row){
							memcpy((void*)ch_ptr_write,
								(void*)ch_ptr_read,
								old_header->record_size);
							ch_ptr_write += old_header->record_size;
						}
						ch_ptr_read += old_header->record_size;
						
					}
					strcat(tablename, ".tab");
							
					if((fhandle = fopen(tablename, "wbc")) == NULL)
					{
					rc = FILE_OPEN_ERROR;
					}
					else
					{
					//tfh = NULL;
					//tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
					
						if (!new_header)
						{
							rc = MEMORY_ERROR;
						}
						else
						{
							//printf("ACCUM: %d", accum);
							//printf("\n");
							
							//new_header->num_records += 1;
							//new_header->tpd_ptr = 0;
							//new_header->file_size = old_header->file_size +32;
							//new_header = new_header - (old_header->file_size + old_header->record_size);
							printf("NEW HEADER FILE SIZE: %d", new_header->file_size);
							printf("\n");
							printf("NUM RECORDS: %d", new_header->num_records);
							
							fwrite(new_header, new_header->file_size, 1, fhandle);
							//fseek(fhandle, 0, SEEK_SET );
							fflush(fhandle);
							fclose(fhandle);
							free(old_header);
							//free(ch_ptr);
						}
					}
					printf("\n");
					printf("DELETE Statement (WHERE) Parsed!\n");
				} else {
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					
				}

			} else {
				printf("COL DOES NOT EXIST: %s\n", cur->tok_string);
				rc = COLUMN_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			
		} else if((cur->next->tok_value == EOC)){
			printf("DELETE ALL ROWS\n");
			cur = cur->next;
					
				
			FILE *fhandle = NULL;
			char filename[MAX_IDENT_LEN + 4];
			

			strcpy(tablename, tab_entry->table_name);
			strcpy(filename, strcat(tab_entry->table_name, ".tab"));
			if ((fhandle = fopen(filename, "rbc")) == NULL)
			{
			printf("Error while opening %s file\n", filename);
			rc = FILE_OPEN_ERROR;
			cur->tok_value = INVALID;
			}
			else
			{
			fstat(fileno(fhandle), &file_stat);
			printf("Old header size = %d\n", file_stat.st_size);
			printf("\n");
			old_header = (table_file_header *)calloc(1, file_stat.st_size);
			// Read the old header information from the tab file.
			fread(old_header, file_stat.st_size, 1, fhandle);
			old_header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
			fclose(fhandle);
			
			int size = old_header->record_size;
			//printf("Size: %d", size);
			//printf("\n");
			//printf("%d", old_header->file_size + size);
			//printf("\n");
			new_header = (table_file_header *)calloc(1, old_header->file_size - (old_header->num_records * old_header->record_size));
			
			memcpy((void*)new_header,
					(void*)old_header,
					old_header->file_size - (old_header->num_records * old_header->record_size));
		
			new_header->num_records = 0;
			new_header->tpd_ptr = 0;
			new_header->file_size = old_header->file_size - (old_header->num_records * old_header->record_size);

		
			//ch_ptr = (char*)new_header;
			//ch_ptr += old_header->file_size;

			strcat(tablename, ".tab");
							
			if((fhandle = fopen(tablename, "wbc")) == NULL)
			{
			rc = FILE_OPEN_ERROR;
			}
			else
			{
			//tfh = NULL;
			//tfh = (table_file_header*)calloc(1, sizeof(table_file_header));
			
				if (!new_header)
				{
					rc = MEMORY_ERROR;
				}
				else
				{
				
					printf("NEW HEADER FILE SIZE: %d", new_header->file_size);
					printf("\n");
					printf("NEW HEADER RECORD SIZE SIZE: %d", new_header->record_size);
					printf("\n");
					printf("NUM RECORDS: %d", new_header->num_records);
					
					fwrite(new_header, new_header->file_size, 1, fhandle);
					//fseek(fhandle, 0, SEEK_SET );
					fflush(fhandle);
					fclose(fhandle);
					free(old_header);
					//free(ch_ptr);
				}
			}
			printf("\n");
			printf("DELETE Statement Parsed!\n");
			}
			
		} else {
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		
	}

	return rc;

}
int sem_update(token_list *t_list){
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	int cur_id_two = 0;
	cd_entry *col_entry = NULL;
	cd_entry *col_entry_two = NULL;
	int i;
	int j;
	int k;
	struct stat file_stat;
	table_file_header *new_header;
	table_file_header *old_header;
	char tablename[MAX_IDENT_LEN+1];
	char *colname;
	char *colname_two;
	int accum = 0;
	char *ch_ptr;
	char *ch_ptr_read;
	char *ch_ptr_write;
	bool col_exists;
	bool col_two_exists;
	int col_type;
	int col_type_two;
	int col_len;
	char *ch_val;
	char *ch_val_cond;
	//bool skip_row;
	bool skip_two;
	bool write_row;
	char* tblname;
	int col_offset = 0;
	//tfh = (table_file_header*)calloc(1, sizeof(table_file_header));cur = t_list;
	cur = t_list;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) 
	{
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
	}
	else 
	{
		printf("TABLE EXISTS\n");
		tblname = cur->tok_string;
		if(cur->next->tok_value == K_SET){
			cur = cur->next;
			if(cur->next != NULL){
				cur = cur->next;
				for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
				{

					col_offset += (col_entry->col_len + 1);
					printf("COL OFF: %d\n", col_offset);
					
					if(strcmp(cur->tok_string, col_entry->col_name) == 0){
						col_exists = true;
						colname = cur->tok_string;
						printf("Col Name: %s\n", col_entry->col_name);
						col_type = col_entry->col_type;
						printf("Col Type: %d\n", col_type);
						col_len = col_entry->col_len;
						cur_id_two = col_entry->col_id;
						col_offset -= (col_entry->col_len);
						printf("COL OFF: %d\n", col_offset);
						break;
						
					} 
				}
				
				if(col_exists){
					cur = cur->next;
					if(cur->tok_value == S_EQUAL){
						cur = cur->next;
						if((cur != NULL) && (cur->next->tok_value != K_WHERE) && (cur->next->tok_class == terminator)){
							printf("UPDATE ALL ROWS\n");
							ch_val = cur->tok_string;
							// Read old header
							FILE *fhandle = NULL;
							char filename[MAX_IDENT_LEN + 4];
							

							strcpy(tablename, tab_entry->table_name);
							strcpy(filename, strcat(tab_entry->table_name, ".tab"));
							if ((fhandle = fopen(filename, "rbc")) == NULL)
							{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
							}
							else
							{
							fstat(fileno(fhandle), &file_stat);
							printf("Old header size = %d\n", file_stat.st_size);
							printf("\n");
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(old_header, file_stat.st_size, 1, fhandle);
							old_header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
							fclose(fhandle);
							}
							// Create new header
							new_header = (table_file_header *)calloc(1, old_header->file_size);
			
							memcpy((void*)new_header,
									(void*)old_header,
									old_header->file_size);
						
							new_header->num_records = old_header->num_records;
							new_header->tpd_ptr = 0;
							new_header->file_size = old_header->file_size;
							// Update column val for every row 
							char * ch_ptr_write = (char*)new_header;
							ch_ptr_write += old_header->record_offset;
							char * ch_ptr_read = (char*)old_header; 
							ch_ptr_read += old_header->record_offset;
						
							int ctr = 0;
							int act_val = old_header->record_size;
							int remainder = 0;

							for(j = 0; j < old_header->num_records; j++){
								for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++) {
											if(col_entry->col_type == T_INT) {
												
												int tmp_int = atoi(ch_val);
												int len = 4;
												
												memcpy((void*)ch_ptr_write,
												(void*)&len,
												1);
												
												ch_ptr_read += 1;
												ch_ptr_write += 1;
												ctr += 1;

												if(strcmp(col_entry->col_name,colname)==0){
													printf("NEW INT: %d\n", tmp_int);
													memcpy((void*)ch_ptr_write,
													(void*)&tmp_int,
													len);
												} else {
													memcpy((void*)ch_ptr_write,
													(void*)ch_ptr_read,
													len);
												}
												
												ch_ptr_read += 4;
												ch_ptr_write += 4;
												ctr += 4;
											}
											else if(col_entry->col_type == T_CHAR) {
												int len = col_entry->col_len;

												memcpy((void*)ch_ptr_write,
												(void*)&len,
												1);

												ch_ptr_read += 1;
												ch_ptr_write += 1;
												ctr += 1;

												if(strcmp(col_entry->col_name,colname)==0){
													char* tmp_char = (char*) malloc(col_entry->col_len + 1);
													memset(tmp_char, '\0', col_entry->col_len + 1);
													memcpy(tmp_char,
														ch_val,
														col_entry->col_len);
													memcpy((void*)ch_ptr_write,
													(void*)tmp_char,
													len);
												} else {
													char* tmp_char = (char*) malloc(col_entry->col_len + 1);
													memset(tmp_char, '\0', col_entry->col_len + 1);
													memcpy(tmp_char,
														ch_ptr_read,
														col_entry->col_len);
													memcpy((void*)ch_ptr_write,
													(void*)tmp_char,
													len);
												}

												ch_ptr_read += col_entry->col_len;
												ch_ptr_write += col_entry->col_len;
												ctr += col_entry->col_len;
											}
											
								}
								if(j == 0){
									remainder =  act_val % ctr;
								}
										
								if(remainder != 0){
										ch_ptr_read += remainder;
										ch_ptr_write += remainder;
								}
							
							}
							// Write to new header
							strcat(tablename, ".tab");
							
							if((fhandle = fopen(tablename, "wbc")) == NULL)
							{
							rc = FILE_OPEN_ERROR;
							}
							else
							{
							
								if (!new_header)
								{
									rc = MEMORY_ERROR;
								}
								else
								{

									printf("NEW HEADER FILE SIZE: %d", new_header->file_size);
									printf("\n");
									printf("NUM RECORDS: %d", new_header->num_records);
									
									fwrite(new_header, new_header->file_size, 1, fhandle);
									//fseek(fhandle, 0, SEEK_SET );
									fflush(fhandle);
									fclose(fhandle);
									free(old_header);
									//free(ch_ptr);
								}
							}
								
							
						} else if((cur != NULL) && (cur->next->tok_value == K_WHERE)){
							ch_val = cur->tok_string;
							printf("CHAR VAL: %s\n", ch_val);
							cur = cur->next->next;
							for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry_two++)
								{
									if(strcmp(cur->tok_string, col_entry_two->col_name) == 0){
										col_two_exists = true;
										colname_two = cur->tok_string;
										printf("Col Name: %s\n", col_entry_two->col_name);
										col_type_two = cur->tok_value;
									} 
								}
							if(col_two_exists){
								cur = cur->next;
								if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_GREATER) || (cur->tok_value == S_LESS)){
									ch_val_cond = cur->next->tok_string;
									printf("CHAR VAL COND: %s\n", ch_val_cond);
									// Read old header
									FILE *fhandle = NULL;
									char filename[MAX_IDENT_LEN + 4];
									

									strcpy(tablename, tab_entry->table_name);
									strcpy(filename, strcat(tab_entry->table_name, ".tab"));
									if ((fhandle = fopen(filename, "rbc")) == NULL)
									{
									printf("Error while opening %s file\n", filename);
									rc = FILE_OPEN_ERROR;
									cur->tok_value = INVALID;
									}
									else
									{
									fstat(fileno(fhandle), &file_stat);
									printf("Old header size = %d\n", file_stat.st_size);
									printf("\n");
									old_header = (table_file_header *)calloc(1, file_stat.st_size);
									fread(old_header, file_stat.st_size, 1, fhandle);
									old_header->tpd_ptr = get_tpd_from_list(tab_entry->table_name);
									fclose(fhandle);
									}
									// Create new header
									new_header = (table_file_header *)calloc(1, old_header->file_size);
					
									memcpy((void*)new_header,
											(void*)old_header,
											old_header->file_size);
								
									new_header->num_records = old_header->num_records;
									new_header->tpd_ptr = 0;
									new_header->file_size = old_header->file_size;

									// Make copy of old file
									char * ch_ptr_write = (char*)new_header;
									ch_ptr_write += old_header->record_offset;
									char * ch_ptr_read = (char*)old_header; 
									ch_ptr_read += old_header->record_offset;
									
									int ctr = 0;
									int act_val = old_header->record_size;
									int remainder = 0;
									int col_off = 0;
									
									for(j = 0; j < old_header->num_records; j++){
										for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, col_entry++) {
													;
												
													if(col_entry->col_type == T_INT) {
														
														int tmp_int = atoi(ch_val);
														int len = 4;
														
														memcpy((void*)ch_ptr_write,
														(void*)&len,
														1);
														
														ch_ptr_read += 1;
														ch_ptr_write += 1;
														ctr += 1;

														memcpy((void*)ch_ptr_write,
														(void*)ch_ptr_read,
														len);
														
														ch_ptr_read += 4;
														ch_ptr_write += 4;
														ctr += 4;
													}
													else if(col_entry->col_type == T_CHAR) {
														int len = col_entry->col_len;

														memcpy((void*)ch_ptr_write,
														(void*)&len,
														1);

														ch_ptr_read += 1;
														ch_ptr_write += 1;
														ctr += 1;

														char* tmp_char = (char*) malloc(col_entry->col_len + 1);
														memset(tmp_char, '\0', col_entry->col_len + 1);
														memcpy(tmp_char,
															ch_ptr_read,
															col_entry->col_len);
														memcpy((void*)ch_ptr_write,
														(void*)tmp_char,
														len);

														ch_ptr_read += col_entry->col_len;
														ch_ptr_write += col_entry->col_len;
														ctr += col_entry->col_len;
													}
													
										}
										if(j == 0){
											remainder =  act_val % ctr;
										}
												
										if(remainder != 0){
												ch_ptr_read += remainder;
												ch_ptr_write += remainder;
										}
									
									}

									// Find matching rows
									std::vector<int> match_col_names;
									int num_rows_updated;

									ch_ptr = (char*)old_header;
									ch_ptr += old_header->record_offset;
									ctr = 0;
									act_val = old_header->record_size;
									remainder = 0;

									for(j = 0; j < old_header->num_records; j++){
										for(i = 0, col_entry_two = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < tab_entry->num_columns; i++, col_entry_two++) {
													if(col_entry_two->col_type == T_INT) {
														ch_ptr += 1;
														ctr += 1;

														int tmp_int = atoi(ch_val_cond);
													
														if(strcmp(col_entry_two->col_name,colname_two)==0){
															if(cur->tok_value == S_EQUAL){
																if(tmp_int == *(int*)ch_ptr){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_LESS){
																if(*(int*)ch_ptr < tmp_int){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_GREATER){
																if(*(int*)ch_ptr > tmp_int){
																	match_col_names.push_back(j);
															
																}
															}
														}
														
														
														ch_ptr += 4;
														ctr += 4;
													}
													else if(col_entry_two->col_type == T_CHAR) {
														ch_ptr += 1;
														ctr += 1;

														
														
														char* tmp_char = (char*) malloc(col_entry_two->col_len + 1);

														memset(tmp_char, '\0', col_entry_two->col_len + 1);
														memcpy(tmp_char,
																ch_ptr,
																col_entry_two->col_len);
														if(strcmp(col_entry_two->col_name,colname_two)==0){
															if(cur->tok_value == S_EQUAL){
																if(strcmp(ch_val_cond, tmp_char) == 0){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_LESS){
																if(strcmp(ch_val_cond, tmp_char) > 0){
																	match_col_names.push_back(j);
																}
															} else if(cur->tok_value == S_GREATER){
																if(strcmp(ch_val_cond, tmp_char) < 0){
																	match_col_names.push_back(j);
																}
															}
														}
														
														
														
														ch_ptr += col_entry_two->col_len;
														ctr += col_entry_two->col_len;
													}
													
										}
										if(j == 0){
											remainder =  act_val % ctr;
										}
												
										if(remainder != 0){
												ch_ptr += remainder;
										}
									
									}
									
									
									num_rows_updated = match_col_names.size();
									printf("Num rows updated: %d\n", num_rows_updated);
									if(num_rows_updated == 0){
										printf("WARNING: No rows updated.\n");
									}
									

									// Update column val for marked rows
									
									//ch_ptr_write += old_header->record_offset;
									ch_ptr_read = (char*)old_header; 
									//ch_ptr_read += old_header->record_offset;
									remainder = 0;
									int total_offset;

									for(j = 0; j < old_header->num_records; j++){
										//printf("j: %d\n", j);
										for(k = 0; k < num_rows_updated; k++){
											if(match_col_names.at(k) == j){
												ch_ptr_write = (char*)new_header;
												total_offset = (old_header->record_size * j) + col_offset + old_header->record_offset;
												ch_ptr_write += total_offset;
												if(col_type == T_INT){
													int tmp_int = atoi(ch_val);

													memcpy((void*)ch_ptr_write,
													(void*)&tmp_int,
													4);
											
												} else if (col_type == T_CHAR){
													char* tmp_char = (char*) malloc(col_len + 1);
													memset(tmp_char, '\0', col_len+ 1);
													memcpy(tmp_char,
														ch_val,
														col_len);

													memcpy((void*)ch_ptr_write,
													(void*)tmp_char,
													col_len);
												}
																								
												
											}
										}
												
										
									}
									
									// Write to new header
									strcat(tablename, ".tab");
									
									if((fhandle = fopen(tablename, "wbc")) == NULL)
									{
									rc = FILE_OPEN_ERROR;
									}
									else
									{
									
										if (!new_header)
										{
											rc = MEMORY_ERROR;
										}
										else
										{

											printf("NEW HEADER FILE SIZE: %d", new_header->file_size);
											printf("\n");
											printf("NUM RECORDS: %d", new_header->num_records);
											
											fwrite(new_header, new_header->file_size, 1, fhandle);
											//fseek(fhandle, 0, SEEK_SET );
											fflush(fhandle);
											fclose(fhandle);
											free(old_header);
											//free(ch_ptr);
										}
									}
								} else {
									rc = INVALID;
									cur->tok_value = INVALID;
								}
							} else {
								rc = INVALID;
								cur->tok_value = INVALID;
							}
							
						} 
						else {
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
					} else {
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
					}
				} else {
					rc = INVALID_COLUMN_NAME;
					cur->tok_value = INVALID;
				}
				
			} else {
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
			}
		} else {
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		
		
	}

	return rc;

}