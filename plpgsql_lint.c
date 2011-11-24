#include "postgres.h"

#include "executor/spi_priv.h"
#include "fmgr.h"
#include "funcapi.h"
#include "plpgsql.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static void lint_func_beg( PLpgSQL_execstate * estate, PLpgSQL_function * func );

static void exec_prepare_plan(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr, int cursorOptions);

static void check_target(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, int varno, TupleDesc tupdesc);
static void check_row_or_rec(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt,
							PLpgSQL_row *row, PLpgSQL_rec *rec,
											TupleDesc tupdesc);

static void prepare_expr(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, PLpgSQL_expr *expr, bool *fresh_plan);
static TupleDesc prepare_tupdesc(PLpgSQL_execstate *estate, PLpgSQL_expr *expr,
								int target_varno,
										bool fresh_plan,
											bool use_element_type,
											bool expand_record,
											bool is_expression);
static TupleDesc prepare_tupdesc_novar(PLpgSQL_execstate *estate, PLpgSQL_expr *expr,
								    bool fresh_plan,
									    bool is_expression);
static TupleDesc prepare_tupdesc_row_or_rec(PLpgSQL_execstate *estate, PLpgSQL_expr *expr,
										PLpgSQL_row *row,
										PLpgSQL_rec *rec,
											bool fresh_plan);

static void assign_tupdesc_dno(PLpgSQL_execstate *estate, int varno, TupleDesc tupdesc);
static void assign_tupdesc_row_or_rec(PLpgSQL_execstate *estate, PLpgSQL_row *row, PLpgSQL_rec *rec, TupleDesc tupdesc);

static void cleanup(bool fresh_plan, PLpgSQL_expr *expr, TupleDesc tupdesc);

static void lint_stmts(PLpgSQL_execstate *estate, PLpgSQL_function *func, List *stmts);
static void lint_stmt(PLpgSQL_execstate *estate, PLpgSQL_function *func, PLpgSQL_stmt *stmt);

static TupleDesc query_get_desc(PLpgSQL_execstate *estate, PLpgSQL_expr *query,
								bool use_element_type,
								bool expand_record,
									bool is_expression);

static void simple_check_expr(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, PLpgSQL_expr *expr);


#if PG_VERSION_NUM < 90000

static Oid exec_get_datum_type(PLpgSQL_execstate *estate,
				    PLpgSQL_datum *datum);

#endif


static PLpgSQL_plugin plugin_funcs = { NULL, lint_func_beg, NULL, NULL, NULL};

/*
 * disable a lint for processing of current function
 *
 */
static bool plpgsql_lint_enable;


void _PG_init(void)
{
	PLpgSQL_plugin ** var_ptr = (PLpgSQL_plugin **) find_rendezvous_variable( "PLpgSQL_plugin" );

	/* Be sure we do initialization only once (should be redundant now) */
	static bool inited = false;

	if (inited)
		return;

	*var_ptr = &plugin_funcs;

#if PG_VERSION_NUM >= 90100

	DefineCustomBoolVariable("plpgsql.enable_lint",
					    "when is true, then plpgsql_lint is active",
					    NULL,
					    &plpgsql_lint_enable,
					    true,
					    PGC_SUSET, 0,
					    NULL, NULL, NULL);

#else

	DefineCustomBoolVariable("plpgsql.enable_lint",
					    "when is true, then plpgsql_lint is active",
					    NULL,
					    &plpgsql_lint_enable,
					    true,
					    PGC_SUSET, 0,
					    NULL, NULL);

#endif

	inited = true;
}

static void
lint_func_beg( PLpgSQL_execstate * estate, PLpgSQL_function * func )
{
	const char *err_text = estate->err_text;

	if (plpgsql_lint_enable)
	{
		int i;
		PLpgSQL_rec *saved_records;
		PLpgSQL_var *saved_vars;
		MemoryContext oldcontext;
		ResourceOwner oldowner;

		/*
		 * inside control a rec and vars variables are modified, so we should to save their
		 * content
		 */
		saved_records = palloc(sizeof(PLpgSQL_rec) * estate->ndatums);
		saved_vars = palloc(sizeof(PLpgSQL_var) * estate->ndatums);

		for (i = 0; i < estate->ndatums; i++)
		{
			if (estate->datums[i]->dtype == PLPGSQL_DTYPE_REC)
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[i];

				saved_records[i].tup = rec->tup;
				saved_records[i].tupdesc = rec->tupdesc;
				saved_records[i].freetup = rec->freetup;
				saved_records[i].freetupdesc = rec->freetupdesc;

				/* don't release a original tupdesc and original tup */
				rec->freetup = false;
				rec->freetupdesc = false;
			}
			else if (estate->datums[i]->dtype == PLPGSQL_DTYPE_VAR)
			{
				PLpgSQL_var *var = (PLpgSQL_var *) estate->datums[i];

				saved_vars[i].value = var->value;
				saved_vars[i].isnull = var->isnull;
				saved_vars[i].freeval = var->freeval;

				var->freeval = false;
			}
		}

		estate->err_text = NULL;

		/*
		 * Raised exception should be trapped in outer functtion. Protection
		 * against outer trap is QUERY_CANCELED exception. 
		 */
		oldcontext = CurrentMemoryContext;
		oldowner = CurrentResourceOwner;

		PG_TRY();
		{
			lint_stmt(estate, func, (PLpgSQL_stmt *) func->action);
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			/* Save error info */
			MemoryContextSwitchTo(oldcontext);
			edata = CopyErrorData();
			FlushErrorState();
			CurrentResourceOwner = oldowner;

			edata->sqlerrcode = ERRCODE_QUERY_CANCELED;
			ReThrowError(edata);
		}
		PG_END_TRY();

		estate->err_text = err_text;
		estate->err_stmt = NULL;

		/* return back a original rec variables */
		for (i = 0; i < estate->ndatums; i++)
		{
			if (estate->datums[i]->dtype == PLPGSQL_DTYPE_REC)
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) estate->datums[i];

				if (rec->freetupdesc)
					FreeTupleDesc(rec->tupdesc);

				rec->tup = saved_records[i].tup;
				rec->tupdesc = saved_records[i].tupdesc;
				rec->freetup = saved_records[i].freetup;
				rec->freetupdesc = saved_records[i].freetupdesc;
			}
			else if (estate->datums[i]->dtype == PLPGSQL_DTYPE_VAR)
			{
				PLpgSQL_var *var = (PLpgSQL_var *) estate->datums[i];

				var->value = saved_vars[i].value;
				var->isnull = saved_vars[i].isnull;
				var->freeval = saved_vars[i].freeval;
			}
		}

		pfree(saved_records);
		pfree(saved_vars);
	}
}

/*
 * Verify lvalue - actually this not compare lvalue against rvalue - that should
 * be next improvent, other improvent should be checking a result type of subscripts
 * expressions.
 */
static void
check_target(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, int varno, TupleDesc tupdesc)
{
	PLpgSQL_datum *target = estate->datums[varno];

	switch (target->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			break;

		case PLPGSQL_DTYPE_REC:
			break;

		case PLPGSQL_DTYPE_ROW:
			{
				check_row_or_rec(estate, stmt, (PLpgSQL_row *) target, NULL, tupdesc);
			}
			break;

		case PLPGSQL_DTYPE_RECFIELD:
			{
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) target;
				PLpgSQL_rec *rec;
				int			fno;

				rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);

				/*
				 * Check that there is already a tuple in the record. We need
				 * that because records don't have any predefined field
				 * structure.
				 */
				if (!HeapTupleIsValid(rec->tup))
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("record \"%s\" is not assigned yet",
								  rec->refname),
						   errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));

				/*
				 * Get the number of the records field to change and the
				 * number of attributes in the tuple.  Note: disallow system
				 * column names because the code below won't cope.
				 */
				fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
				if (fno <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("record \"%s\" has no field \"%s\"",
									rec->refname, recfield->fieldname)));
			}
			break;

		case PLPGSQL_DTYPE_ARRAYELEM:
			{
				/*
				 * Target is an element of an array
				 */
				int			nsubscripts;
				Oid		arrayelemtypeid;

				/*
				 * To handle constructs like x[1][2] := something, we have to
				 * be prepared to deal with a chain of arrayelem datums. Chase
				 * back to find the base array datum, and save the subscript
				 * expressions as we go.  (We are scanning right to left here,
				 * but want to evaluate the subscripts left-to-right to
				 * minimize surprises.)
				 */
				nsubscripts = 0;
				do
				{
					PLpgSQL_arrayelem *arrayelem = (PLpgSQL_arrayelem *) target;

					if (nsubscripts++ >= MAXDIM)
						ereport(ERROR,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
										nsubscripts + 1, MAXDIM)));

					simple_check_expr(estate, stmt, arrayelem->subscript);

					target = estate->datums[arrayelem->arrayparentno];
				} while (target->dtype == PLPGSQL_DTYPE_ARRAYELEM);

				arrayelemtypeid = get_element_type(exec_get_datum_type(estate, target));

				if (!OidIsValid(arrayelemtypeid))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("subscripted object is not an array")));
			}
			break;
	}

	return;
}

/*
 * Check composed lvalue
 */
static void
check_row_or_rec(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt,
							PLpgSQL_row *row, PLpgSQL_rec *rec,
											TupleDesc tupdesc)
{
	int fnum;

	/* there are nothing to check on rec now */
	if (row != NULL)
	{
		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			if (row->varnos[fnum] < 0)
				continue;
	
			check_target(estate, stmt, row->varnos[fnum], tupdesc);
		}
	}
}

/*
 * Prepare expression's plan. When there are plan generated by plpgsql interpret
 * then set flag fresh_plan to false and returns.
 */
static void
prepare_expr(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, PLpgSQL_expr *expr, bool *fresh_plan)
{
	int cursorOptions = 0;

	Assert(stmt != NULL && expr != NULL);

	if (expr->plan != NULL)
	{
		*fresh_plan = false;
		return;
	}

	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_OPEN:
			{
				PLpgSQL_stmt_open *stmt_open = (PLpgSQL_stmt_open *) stmt;
				PLpgSQL_var *curvar = (PLpgSQL_var *) estate->datums[stmt_open->curvar];

				cursorOptions = curvar->cursor_options;
			}
			break;

		case PLPGSQL_STMT_FORC:
			{
				PLpgSQL_stmt_forc *stmt_forc = (PLpgSQL_stmt_forc *) stmt;
				PLpgSQL_var *curvar = (PLpgSQL_var *) estate->datums[stmt_forc->curvar];

				/*
				 * change a cursorOption only when this call is related to
				 * curvar->cursor_explicit_expr
				 */
				if (curvar->cursor_explicit_expr == expr)
					cursorOptions = curvar->cursor_options;
			}
			break;
	}

	exec_prepare_plan(estate, expr, cursorOptions);

	*fresh_plan = true;

}

/*
 * Prepare tuple descriptor based on expression result. This process has a some overhed, so it
 * returns NULL, when descriptor is not necessary.
 */
static TupleDesc
prepare_tupdesc(PLpgSQL_execstate *estate, PLpgSQL_expr *expr,
								int target_varno,
										bool fresh_plan,
											bool use_element_type,
											bool expand_record,
											bool is_expression)
{
	PLpgSQL_datum *target = estate->datums[target_varno];
	TupleDesc tupdesc = NULL;
    
	/*
	 * do tupdesc when:
	 *       target_varno is DTYPE_REC
	 *       is fresh_plan and is expression
	 */
	if ((fresh_plan && is_expression) || target->dtype == PLPGSQL_DTYPE_REC)
	{
		tupdesc = query_get_desc(estate, expr, use_element_type, expand_record, is_expression);
	}

	return tupdesc;
}

/*
 * Prepare tuple desc when target is record or plan is fresh
 */
static TupleDesc
prepare_tupdesc_row_or_rec(PLpgSQL_execstate *estate, PLpgSQL_expr *expr,
									PLpgSQL_row *row,
									PLpgSQL_rec *rec,
										bool fresh_plan)
{
	TupleDesc tupdesc = NULL;

	/*
	 * do tupdesc when:
	 *       target_varno is DTYPE_REC
	 *       is fresh_plan and is expression
	 */
	if (fresh_plan || rec != NULL)
	{
		tupdesc = query_get_desc(estate, expr, false, false, false);
	}

	return tupdesc;
}


/*
 * same as above, but not used for assign tupledesc to record var
 */
static TupleDesc
prepare_tupdesc_novar(PLpgSQL_execstate *estate, PLpgSQL_expr *expr,
								    bool fresh_plan,
									    bool is_expression)
{
	TupleDesc tupdesc = NULL;

	if (fresh_plan && is_expression)
	{
		tupdesc = query_get_desc(estate, expr, false, false, is_expression);

		if (is_expression)
			if (tupdesc->natts != 1)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						  errmsg_plural("query \"%s\" returned %d column",
								   "query \"%s\" returned %d columns",
								   tupdesc->natts,
								   expr->query,
								   tupdesc->natts)));
	}

	return tupdesc;
}

/*
 * Check of simple expression. Checked expression is not used as source for any target
 */
static void
simple_check_expr(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt, PLpgSQL_expr *expr)
{
	bool fresh_plan;
	TupleDesc	tupdesc;

	if (expr != NULL)
	{
		prepare_expr(estate, stmt, expr, &fresh_plan);
		tupdesc = prepare_tupdesc_novar(estate, expr, fresh_plan, true);
		cleanup(fresh_plan, expr, tupdesc);
	}
}

#if PG_VERSION_NUM < 90000

/*
 * Similar function exec_get_datum_type is in 9nth line
 */
static Oid
exec_get_datum_type(PLpgSQL_execstate *estate,
				    PLpgSQL_datum *datum)
{
	Oid typoid = InvalidOid;

	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			typoid = ((PLpgSQL_var *) datum)->datatype->typoid;
			break;

		case PLPGSQL_DTYPE_ROW:
			typoid = ((PLpgSQL_row *) datum)->rowtupdesc->tdtypeid;
			break;

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) datum;

				if (!HeapTupleIsValid(rec->tup))
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("record \"%s\" is not assigned yet",
								  rec->refname),
						   errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
				Assert(rec->tupdesc != NULL);
				/* Make sure we have a valid type/typmod setting */
				BlessTupleDesc(rec->tupdesc);

				typoid = rec->tupdesc->tdtypeid;
			}
			break;

		case PLPGSQL_DTYPE_RECFIELD:
			{
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) datum;
				PLpgSQL_rec *rec;
				int			fno;

				rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);
				if (!HeapTupleIsValid(rec->tup))
					ereport(ERROR,
						  (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						   errmsg("record \"%s\" is not assigned yet",
								  rec->refname),
						   errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));
				fno = SPI_fnumber(rec->tupdesc, recfield->fieldname);
				if (fno == SPI_ERROR_NOATTRIBUTE)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("record \"%s\" has no field \"%s\"",
									rec->refname, recfield->fieldname)));
				typoid = SPI_gettypeid(rec->tupdesc, fno);
			}
			break;

		case PLPGSQL_DTYPE_TRIGARG:
			typoid = TEXTOID;
			break;
	}

	return typoid;
}

#endif


/* ----------
 * Generate a prepared plan - this is copy from pl_exec.c
 * ----------
 */
static void
exec_prepare_plan(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr, int cursorOptions)
{
#if PG_VERSION_NUM >= 90000

	SPIPlanPtr	plan;

	/*
	 * The grammar can't conveniently set expr->func while building the parse
	 * tree, so make sure it's set before parser hooks need it.
	 */
	expr->func = estate->func;

	/*
	 * Generate and save the plan
	 */
	plan = SPI_prepare_params(expr->query,
							  (ParserSetupHook) plpgsql_parser_setup,
							  (void *) expr,
							  cursorOptions);
	if (plan == NULL)
	{
		/* Some SPI errors deserve specific error messages */
		switch (SPI_result)
		{
			case SPI_ERROR_COPY:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot COPY to/from client in PL/pgSQL")));
			case SPI_ERROR_TRANSACTION:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot begin/end transactions in PL/pgSQL"),
						 errhint("Use a BEGIN block with an EXCEPTION clause instead.")));
			default:
				elog(ERROR, "SPI_prepare_params failed for \"%s\": %s",
					 expr->query, SPI_result_code_string(SPI_result));
		}
	}

	expr->plan = SPI_saveplan(plan);
	SPI_freeplan(plan);

#else

	int			i;
	SPIPlanPtr	plan;
	Oid		   *argtypes;

	/*
	 * We need a temporary argtypes array to load with data. (The finished
	 * plan structure will contain a copy of it.)
	 */
	argtypes = (Oid *) palloc(expr->nparams * sizeof(Oid));

	for (i = 0; i < expr->nparams; i++)
	{
		argtypes[i] = exec_get_datum_type(estate, estate->datums[expr->params[i]]);
	}

	/*
	 * Generate and save the plan
	 */
	plan = SPI_prepare_cursor(expr->query, expr->nparams, argtypes,
							  cursorOptions);
	if (plan == NULL)
	{
		/* Some SPI errors deserve specific error messages */
		switch (SPI_result)
		{
			case SPI_ERROR_COPY:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot COPY to/from client in PL/pgSQL")));
			case SPI_ERROR_TRANSACTION:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot begin/end transactions in PL/pgSQL"),
						 errhint("Use a BEGIN block with an EXCEPTION clause instead.")));
			default:
				elog(ERROR, "SPI_prepare_cursor failed for \"%s\": %s",
					 expr->query, SPI_result_code_string(SPI_result));
		}
	}

	expr->plan = SPI_saveplan(plan);
	SPI_freeplan(plan);

	pfree(argtypes);

#endif

}

/*
 * call a lint_stmt for any statement in list
 *
 */
static void
lint_stmts(PLpgSQL_execstate *estate,
					PLpgSQL_function *func,
					List *stmts)
{
	ListCell *lc;

	foreach(lc, stmts)
	{
		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(lc);

		lint_stmt(estate, func, stmt);
	}
}

/*
 * walk over all statements tree
 *
 */
static void
lint_stmt(PLpgSQL_execstate *estate,
					PLpgSQL_function *func,
					PLpgSQL_stmt *stmt)
{
	bool		   fresh_plan;
	TupleDesc	   tupdesc = NULL;
	ListCell *l;

	if (stmt == NULL)
		return;

	estate->err_stmt = stmt;

	switch ((enum PLpgSQL_stmt_types) stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
			{
				PLpgSQL_stmt_block *stmt_block = (PLpgSQL_stmt_block *) stmt;
				int		i;
				PLpgSQL_datum		*d;

				for (i = 0; i < stmt_block->n_initvars; i++)
				{
					d = func->datums[stmt_block->initvarnos[i]];

					if (d->dtype == PLPGSQL_DTYPE_VAR)
					{
						PLpgSQL_var *var = (PLpgSQL_var *) d;

						simple_check_expr(estate, stmt, var->default_val);
					}
				}

				lint_stmts(estate, func, stmt_block->body);
				
				if (stmt_block->exceptions)
				{
					foreach(l, stmt_block->exceptions->exc_list)
					{
						lint_stmts(estate, func, ((PLpgSQL_exception *) lfirst(l))->action);
					}
				}
			}
			break;

		case PLPGSQL_STMT_ASSIGN:
			{
				PLpgSQL_stmt_assign *stmt_assign = (PLpgSQL_stmt_assign *) stmt;

				/* prepare plan if desn't exist yet */
				prepare_expr(estate, stmt, stmt_assign->expr, &fresh_plan);

				/*
				 * Tuple descriptor is necessary when we would to check expression
				 * first time or when target is record.
				 */
				tupdesc = prepare_tupdesc(estate, stmt_assign->expr, stmt_assign->varno,
											fresh_plan,
												false,		/* no element type */
												true,		/* expand record */
												true);		/* is expression */

				/* check target, ensure target can get a result */
				check_target(estate, stmt, stmt_assign->varno, tupdesc);

				/* assign a tupdesc to record variable */
				assign_tupdesc_dno(estate, stmt_assign->varno, tupdesc);

				/* cleanup fresh plan and tuple descriptor */
				cleanup(fresh_plan, stmt_assign->expr, tupdesc);
			}
			break;

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;

#if PG_VERSION_NUM >= 90200

				ListCell *l;

#endif

				simple_check_expr(estate, stmt, stmt_if->cond);

#if PG_VERSION_NUM >= 90200

				lint_stmts(estate, func, stmt_if->then_body);

				foreach(l, stmt_if->elsif_list)
				{
					PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

					simple_check_expr(estate, stmt, elif->cond);
					lint_stmts(estate, func, elif->stmts);
				}

				lint_stmts(estate, func, stmt_if->else_body);

#else

				lint_stmts(estate, func, stmt_if->true_body);
				lint_stmts(estate, func, stmt_if->false_body);

#endif

			}
			break;

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
				Oid result_oid;

				if (stmt_case->t_expr != NULL)
				{
					PLpgSQL_var *t_var = (PLpgSQL_var *) estate->datums[stmt_case->t_varno];

					prepare_expr(estate, stmt, stmt_case->t_expr, &fresh_plan);

					/* we need tupdesc everytime */
					tupdesc = prepare_tupdesc_novar(estate, stmt_case->t_expr, true, true);
					result_oid = tupdesc->attrs[0]->atttypid;

					/*
					 * When expected datatype is different from real, change it. Note that
					 * what we're modifying here is an execution copy of the datum, so
					 * this doesn't affect the originally stored function parse tree.
					 */

					if (t_var->datatype->typoid != result_oid)

#if PG_VERSION_NUM >= 90100

						t_var->datatype = plpgsql_build_datatype(result_oid,
														 -1,
													   estate->func->fn_input_collation);
#else

						t_var->datatype = plpgsql_build_datatype(result_oid, -1);

#endif

					cleanup(fresh_plan, stmt_case->t_expr,tupdesc);
				}

				foreach(l, stmt_case->case_when_list)
				{
					PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

					simple_check_expr(estate, stmt, cwt->expr);
					lint_stmts(estate, func, cwt->stmts);
				}

				lint_stmts(estate, func, stmt_case->else_stmts);

			}
			break;

		case PLPGSQL_STMT_LOOP:
			lint_stmts(estate, func, ((PLpgSQL_stmt_loop *) stmt)->body);
			break;

		case PLPGSQL_STMT_WHILE:
			{
				PLpgSQL_stmt_while *stmt_while = (PLpgSQL_stmt_while *) stmt;

				simple_check_expr(estate, stmt, stmt_while->cond);
				lint_stmts(estate, func, stmt_while->body);
			}
			break;

		case PLPGSQL_STMT_FORI:
			{
				PLpgSQL_stmt_fori *stmt_fori = (PLpgSQL_stmt_fori *) stmt;

				simple_check_expr(estate, stmt, stmt_fori->lower);
				simple_check_expr(estate, stmt, stmt_fori->upper);
				simple_check_expr(estate, stmt, stmt_fori->step);

				lint_stmts(estate, func, stmt_fori->body);
			}
			break;

		case PLPGSQL_STMT_FORS:
			{
				PLpgSQL_stmt_fors *stmt_fors = (PLpgSQL_stmt_fors *) stmt;

				prepare_expr(estate, stmt, stmt_fors->query, &fresh_plan);
				tupdesc = prepare_tupdesc_row_or_rec(estate,
									stmt_fors->query,
											stmt_fors->row,
											stmt_fors->rec,
												fresh_plan);
				check_row_or_rec(estate, stmt,
							stmt_fors->row, stmt_fors->rec,
											    tupdesc);
				assign_tupdesc_row_or_rec(estate, stmt_fors->row, stmt_fors->rec, tupdesc);

				lint_stmts(estate, func, stmt_fors->body);

				cleanup(fresh_plan, stmt_fors->query, tupdesc);
			}
			break;

		case PLPGSQL_STMT_FORC:
			{
				PLpgSQL_stmt_forc *stmt_forc = (PLpgSQL_stmt_forc *) stmt;
				PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_forc->curvar];

				simple_check_expr(estate, stmt, stmt_forc->argquery);

				if (var->cursor_explicit_expr != NULL)
				{
					prepare_expr(estate, stmt, var->cursor_explicit_expr, &fresh_plan);
					tupdesc = prepare_tupdesc_row_or_rec(estate,
										var->cursor_explicit_expr,
												stmt_forc->row,
												stmt_forc->rec,
													fresh_plan);
					check_row_or_rec(estate, stmt,
								stmt_forc->row, stmt_forc->rec,
												    tupdesc);
					assign_tupdesc_row_or_rec(estate, stmt_forc->row, stmt_forc->rec, tupdesc);
				}

				lint_stmts(estate, func, stmt_forc->body);
				cleanup(fresh_plan, var->cursor_explicit_expr, tupdesc);
			}
			break;

		case PLPGSQL_STMT_DYNFORS:
			{
				PLpgSQL_stmt_dynfors * stmt_dynfors = (PLpgSQL_stmt_dynfors *) stmt;

				if (stmt_dynfors->rec != NULL)
					elog(ERROR, "cannot determinate a result of dynamic SQL");

				simple_check_expr(estate, stmt, stmt_dynfors->query);

				foreach(l, stmt_dynfors->params)
				{
					simple_check_expr(estate, stmt, (PLpgSQL_expr *) lfirst(l));
				}

				lint_stmts(estate, func, stmt_dynfors->body);
			}
			break;

#if PG_VERSION_NUM >= 90100

		case PLPGSQL_STMT_FOREACH_A:
			{
				PLpgSQL_stmt_foreach_a *stmt_foreach_a = (PLpgSQL_stmt_foreach_a *) stmt;

				prepare_expr(estate, stmt, stmt_foreach_a->expr, &fresh_plan);
				tupdesc = prepare_tupdesc(estate, stmt_foreach_a->expr, stmt_foreach_a->varno,
											fresh_plan,
												true,		/* element type */
												false,		/* expand record */
												true);		/* is expression */

				check_target(estate, stmt, stmt_foreach_a->varno, tupdesc);
				assign_tupdesc_dno(estate, stmt_foreach_a->varno, tupdesc);
				cleanup(fresh_plan, stmt_foreach_a->expr, tupdesc);

				lint_stmts(estate, func, stmt_foreach_a->body);
			}
			break;

#endif

		case PLPGSQL_STMT_EXIT:
			simple_check_expr(estate, stmt, ((PLpgSQL_stmt_exit *) stmt)->cond);
			break;

		case PLPGSQL_STMT_PERFORM:
			{
				PLpgSQL_stmt_perform *stmt_perform = (PLpgSQL_stmt_perform *) stmt;

				prepare_expr(estate, stmt, stmt_perform->expr, &fresh_plan);
				cleanup(fresh_plan, stmt_perform->expr, NULL);
			}
			break;

			simple_check_expr(estate, stmt, ((PLpgSQL_stmt_perform *) stmt)->expr);
			break;

		case PLPGSQL_STMT_RETURN:
			simple_check_expr(estate, stmt, ((PLpgSQL_stmt_return *) stmt)->expr);
			break;

		case PLPGSQL_STMT_RETURN_NEXT:
			simple_check_expr(estate, stmt, ((PLpgSQL_stmt_return_next *) stmt)->expr);
			break;

		case PLPGSQL_STMT_RETURN_QUERY:
			{
				PLpgSQL_stmt_return_query *stmt_rq = (PLpgSQL_stmt_return_query *) stmt;

				simple_check_expr(estate, stmt, stmt_rq->query);
				simple_check_expr(estate, stmt, stmt_rq->dynquery);

				foreach(l, stmt_rq->params)
				{
					simple_check_expr(estate, stmt, (PLpgSQL_expr *) lfirst(l));
				}
			}
			break;

		case PLPGSQL_STMT_RAISE:
			{
				PLpgSQL_stmt_raise *stmt_raise = (PLpgSQL_stmt_raise *) stmt;
				ListCell *current_param;
				char *cp;

				foreach(l, stmt_raise->params)
				{
					simple_check_expr(estate, stmt, (PLpgSQL_expr *) lfirst(l));
				}

				foreach(l, stmt_raise->options)
				{
					simple_check_expr(estate, stmt, (PLpgSQL_expr *) lfirst(l));
				}

				current_param = list_head(stmt_raise->params);

				/* we can skip this test, when we identify a second loop */
				if (!(current_param != NULL && (((PLpgSQL_expr *) lfirst(current_param))->plan != NULL)))
				{
					/* ensure any single % has a own parameter */
					if (stmt_raise->message != NULL)
					{
						for (cp = stmt_raise->message; *cp; cp++)
						{
							if (cp[0] == '%')
							{
								if (cp[1] == '%')
								{
									cp++;
									continue;
								}

								if (current_param == NULL)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("too few parameters specified for RAISE")));

								current_param = lnext(current_param);
							}
						}
					}

					if (current_param != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("too many parameters specified for RAISE")));
				}
			}
			break;

		case PLPGSQL_STMT_EXECSQL:
			{
				PLpgSQL_stmt_execsql *stmt_execsql = (PLpgSQL_stmt_execsql *) stmt;

				prepare_expr(estate, stmt, stmt_execsql->sqlstmt, &fresh_plan);
				if (stmt_execsql->into)
					tupdesc = prepare_tupdesc_row_or_rec(estate,
										stmt_execsql->sqlstmt,
											stmt_execsql->row,
											stmt_execsql->rec,
												fresh_plan);

				/* check target, ensure target can get a result */
				check_row_or_rec(estate, stmt,
							stmt_execsql->row, stmt_execsql->rec,
											    tupdesc);
				assign_tupdesc_row_or_rec(estate, stmt_execsql->row, stmt_execsql->rec, tupdesc);
				cleanup(fresh_plan, stmt_execsql->sqlstmt, tupdesc);
			}
			break;

		case PLPGSQL_STMT_DYNEXECUTE:
			{
				PLpgSQL_stmt_dynexecute *stmt_dynexecute = (PLpgSQL_stmt_dynexecute *) stmt;

				simple_check_expr(estate, stmt, stmt_dynexecute->query);

				foreach(l, stmt_dynexecute->params)
				{
					simple_check_expr(estate, stmt, (PLpgSQL_expr *) lfirst(l));
				}

				if (stmt_dynexecute->into)
				{
					if (stmt_dynexecute->rec != NULL)
						elog(ERROR, "cannot determinate a result of dynamic SQL");

					check_row_or_rec(estate, stmt, stmt_dynexecute->row, stmt_dynexecute->rec, NULL);
				}
			}
			break;

		case PLPGSQL_STMT_OPEN:
			{
				PLpgSQL_stmt_open *stmt_open = (PLpgSQL_stmt_open *) stmt;
				PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_open->curvar];

				simple_check_expr(estate, stmt, var->cursor_explicit_expr);
				simple_check_expr(estate, stmt, stmt_open->query);
				simple_check_expr(estate, stmt, stmt_open->dynquery);
				simple_check_expr(estate, stmt, stmt_open->argquery);

#if PG_VERSION_NUM >= 90000

				foreach(l, stmt_open->params)
				{
					simple_check_expr(estate, stmt, (PLpgSQL_expr *) lfirst(l));
				}

#endif
			}
			break;

		case PLPGSQL_STMT_GETDIAG:
			{
				PLpgSQL_stmt_getdiag *stmt_getdiag = (PLpgSQL_stmt_getdiag *) stmt;
				ListCell *lc;

				foreach(lc, stmt_getdiag->diag_items)
				{
					PLpgSQL_diag_item *diag_item = (PLpgSQL_diag_item *) lfirst(lc);

					check_target(estate, stmt, diag_item->target, NULL);
				}
			}
			break;

		case PLPGSQL_STMT_FETCH:
			{
				PLpgSQL_stmt_fetch *stmt_fetch = (PLpgSQL_stmt_fetch *) stmt;
				PLpgSQL_var *curvar = (PLpgSQL_var *)(estate->datums[stmt_fetch->curvar]);

				if (curvar != NULL && curvar->cursor_explicit_expr != NULL)
				{
					prepare_expr(estate, stmt, curvar->cursor_explicit_expr, &fresh_plan);
					tupdesc = prepare_tupdesc_row_or_rec(estate,
										curvar->cursor_explicit_expr,
												stmt_fetch->row,
												stmt_fetch->rec,
													fresh_plan);
					check_row_or_rec(estate, stmt,
								stmt_fetch->row, stmt_fetch->rec,
												    tupdesc);
					assign_tupdesc_row_or_rec(estate, stmt_fetch->row, stmt_fetch->rec, tupdesc);
					cleanup(fresh_plan, curvar->cursor_explicit_expr, tupdesc);
				}
			}
			break;

		case PLPGSQL_STMT_CLOSE:
			break;

		default:
			elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
			return; /* be compiler quite */
	}
}

/*
 * Returns a tuple descriptor of planned query
 */
static TupleDesc 
query_get_desc(PLpgSQL_execstate *estate,
						PLpgSQL_expr *query,
							bool use_element_type,
							bool expand_record,
								bool is_expression)
{
	TupleDesc tupdesc = NULL;
	CachedPlanSource *plansource = NULL;

	if (query->plan != NULL)
	{
		SPIPlanPtr plan = query->plan;

		if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
			elog(ERROR, "cached plan is not valid plan");

		if (list_length(plan->plancache_list) != 1)
			elog(ERROR, "plan is not single execution plan");

		plansource = (CachedPlanSource *) linitial(plan->plancache_list);

		tupdesc = CreateTupleDescCopy(plansource->resultDesc);
	}
	else
		elog(ERROR, "there are no plan for query: \"%s\"",
							    query->query);

	/*
	 * try to get a element type, when result is a array (used with FOREACH ARRAY stmt)
	 */
	if (use_element_type)
	{
		Oid elemtype;
		TupleDesc elemtupdesc;

		/* result should be a array */
		if (tupdesc->natts != 1)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg_plural("query \"%s\" returned %d column",
							   "query \"%s\" returned %d columns",
							   tupdesc->natts,
							   query->query,
							   tupdesc->natts)));

		/* check the type of the expression - must be an array */
		elemtype = get_element_type(tupdesc->attrs[0]->atttypid);
		if (!OidIsValid(elemtype))
			ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("FOREACH expression must yield an array, not type %s",
						format_type_be(tupdesc->attrs[0]->atttypid))));

		/* we can't know typmod now */
		elemtupdesc = lookup_rowtype_tupdesc_noerror(elemtype, -1, true);
		if (elemtupdesc != NULL)
		{
			FreeTupleDesc(tupdesc);
			tupdesc = CreateTupleDescCopy(elemtupdesc);
			ReleaseTupleDesc(elemtupdesc);
		}
		else
			elog(ERROR, "cannot to identify real type for record type variable");
	}

	if (is_expression && tupdesc->natts != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
			  errmsg_plural("query \"%s\" returned %d column",
				   "query \"%s\" returned %d columns",
				   tupdesc->natts,
				   query->query,
				   tupdesc->natts)));

	/*
	 * One spacial case is when record is assigned to composite type, then 
	 * we should to unpack composite type.
	 */
	if (tupdesc->tdtypeid == RECORDOID &&
			tupdesc->tdtypmod == -1 &&
			tupdesc->natts == 1 && expand_record)
	{
		TupleDesc unpack_tupdesc;

		unpack_tupdesc = lookup_rowtype_tupdesc_noerror(tupdesc->attrs[0]->atttypid,
								tupdesc->attrs[0]->atttypmod,
											    true);
		if (unpack_tupdesc != NULL)
		{
			FreeTupleDesc(tupdesc);
			tupdesc = CreateTupleDescCopy(unpack_tupdesc);
			ReleaseTupleDesc(unpack_tupdesc);
		}
	}

	/*
	 * There is special case, when returned tupdesc contains only
	 * unpined record: rec := func_with_out_parameters(). IN this case
	 * we must to dig more deep - we have to find oid of function and
	 * get their parameters,
	 *
	 * This is support for assign statement
	 *     recvar := func_with_out_parameters(..)
	 */
	if (tupdesc->tdtypeid == RECORDOID &&
			tupdesc->tdtypmod == -1 &&
			tupdesc->natts == 1 &&
			tupdesc->attrs[0]->atttypid == RECORDOID &&
			tupdesc->attrs[0]->atttypmod == -1 &&
			expand_record)
	{
		PlannedStmt *_stmt;
		Plan		*_plan;
		TargetEntry *tle;

		/*
		 * When tupdesc is related to unpined record, we will try
		 * to check plan if it is just function call and if it is
		 * then we can try to derive a tupledes from function's
		 * description.
		 */

#if PG_VERSION_NUM >= 90200

		CachedPlan *cplan;

		cplan = GetCachedPlan(plansource, NULL, true);
		_stmt = (PlannedStmt *) linitial(cplan->stmt_list);

#else

		_stmt = (PlannedStmt *) linitial(plansource->plan->stmt_list);

#endif

		if (IsA(_stmt, PlannedStmt) && _stmt->commandType == CMD_SELECT)
		{
			_plan = _stmt->planTree;
			if (IsA(_plan, Result) && list_length(_plan->targetlist) == 1)
			{
				tle = (TargetEntry *) linitial(_plan->targetlist);
				if (((Node *) tle->expr)->type == T_FuncExpr)
				{
					FuncExpr *fn = (FuncExpr *) tle->expr;
					FmgrInfo flinfo;
					FunctionCallInfoData fcinfo;
					TupleDesc rd;
					Oid		rt;

					fmgr_info(fn->funcid, &flinfo);
					flinfo.fn_expr = (Node *) fn;
					fcinfo.flinfo = &flinfo;

					get_call_result_type(&fcinfo, &rt, &rd);
					if (rd == NULL)
						elog(ERROR, "function does not return composite type is not possible to identify composite type");

					FreeTupleDesc(tupdesc);
					BlessTupleDesc(rd);

					tupdesc = rd;
				}
			}
		}

#if PG_VERSION_NUM >= 90200

		ReleaseCachedPlan(cplan, true);

#endif
	}

	return tupdesc;
}

/*
 * Sometime we must initialize a unknown record variable with NULL
 * of type that is derived from some plan. This is necessary for later
 * using a rec variable. Last parameter 'use_element_type' is true, when
 * we would to assign a element type of result array.
 *
 */
static void
assign_tupdesc_row_or_rec(PLpgSQL_execstate *estate, PLpgSQL_row *row, PLpgSQL_rec *rec, TupleDesc tupdesc)
{
	bool	   *nulls;
	HeapTuple  tup;

	if (rec != NULL)
	{
		PLpgSQL_rec *target = (PLpgSQL_rec *)(estate->datums[rec->dno]);

		if (tupdesc == NULL)
			elog(ERROR, "tuple descriptor is empty");

		if (target->freetup)
			heap_freetuple(target->tup);

		if (rec->freetupdesc)
			FreeTupleDesc(target->tupdesc);

		/* initialize rec by NULLs */
		nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
		memset(nulls, true, tupdesc->natts * sizeof(bool));

		target->tupdesc = CreateTupleDescCopy(tupdesc);
		target->freetupdesc = true;

		tup = heap_form_tuple(tupdesc, NULL, nulls);
		if (HeapTupleIsValid(tup))
		{
			target->tup = tup;
			target->freetup = true;
		}
		else
			elog(ERROR, "cannot to build valid composite value");
	}
}

static void
assign_tupdesc_dno(PLpgSQL_execstate *estate, int varno, TupleDesc tupdesc)
{
	PLpgSQL_datum *target = estate->datums[varno];

	if (target->dtype == PLPGSQL_DTYPE_REC)
		assign_tupdesc_row_or_rec(estate, NULL, (PLpgSQL_rec *) target, tupdesc);
}

static void
cleanup(bool fresh_plan, PLpgSQL_expr *expr, TupleDesc tupdesc)
{
	Assert(expr->plan != NULL);

	if (tupdesc != NULL)
		ReleaseTupleDesc(tupdesc);

	if (fresh_plan)
	{
		SPI_freeplan(expr->plan);
		expr->plan = NULL;
	}
}
