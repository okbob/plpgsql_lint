#include "postgres.h"

#include "executor/spi_priv.h"
#include "fmgr.h"
#include "funcapi.h"
#include "plpgsql.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static void lint_func_beg( PLpgSQL_execstate * estate, PLpgSQL_function * func );
static void assign_result_desc(PLpgSQL_execstate *estate,
				    PLpgSQL_stmt *stmt,
					    PLpgSQL_rec *rec,
						    PLpgSQL_expr *query,
							    bool use_element_type);
static bool plpgsql_lint_expr_walker(PLpgSQL_function *func,
				PLpgSQL_stmt *stmt,
					bool (*expr_walker)(),
								void *context);
static bool plpgsql_lint_expr_prepare_plan(PLpgSQL_stmt *stmt, PLpgSQL_expr *expr, void *context);

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
	if (plpgsql_lint_enable)
	{
		int i;
		PLpgSQL_rec *saved_records;
		PLpgSQL_var *saved_vars;

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

		plpgsql_lint_expr_walker(func, (PLpgSQL_stmt *) func->action,
							    plpgsql_lint_expr_prepare_plan, (void *) estate);

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


/* ----------
 * Generate a prepared plan - this is copy from pl_exec.c
 * ----------
 */
static void
exec_prepare_plan(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr, int cursorOptions)
{
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
}

/*
 * call a plpgsql_lint_expr_walker for any statement in list
 *
 */
static bool
plpgsql_lint_expr_walker_list(PLpgSQL_function *func, List *stmts,
					bool (*expr_walker)(),
								void *context)
{
	ListCell *lc;

	foreach(lc, stmts)
	{
		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(lc);

		if (plpgsql_lint_expr_walker(func, stmt, expr_walker, context))
			return true;
	}
	return false;
}

/*
 * walk over all expressions inside statements tree
 *
 * stmt_walker is function called for every stmt and should be NULL
 *
 */
static bool
plpgsql_lint_expr_walker(PLpgSQL_function *func,
				PLpgSQL_stmt *stmt,
					bool (*expr_walker)(),
								void *context)
{
	ListCell *l;

	if (stmt == NULL)
		return false;

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

					switch(d->dtype)
					{
						case PLPGSQL_DTYPE_VAR:
							{
								PLpgSQL_var *var = (PLpgSQL_var *) d;

								if (expr_walker(stmt, var->default_val, context))
									return true;

								/*
								 * theoretically there is place for walk over var->cursor_explicit_expr,
								 * but we would not to call process too early. In this moment a some 
								 * record parameters should be unknown. So we will wait on better moment
								 *
								 * if (expr_walker(stmt, var->cursor_explicit_expr, context))
								 * 	return true;
								 */
							}
							break;
						case PLPGSQL_DTYPE_ROW:
						case PLPGSQL_DTYPE_REC:
						case PLPGSQL_DTYPE_RECFIELD:
							break;
						case PLPGSQL_DTYPE_ARRAYELEM:
							if (expr_walker(stmt, ((PLpgSQL_arrayelem *) d)->subscript, context))
								return true;
						default:
							elog(ERROR, "unrecognized data type: %d", d->dtype);
					}
				}

				if (plpgsql_lint_expr_walker_list(func, stmt_block->body, expr_walker, context))
					return true;

				if (stmt_block->exceptions)
				{
					foreach(l, stmt_block->exceptions->exc_list)
					{
						if (plpgsql_lint_expr_walker_list(func, ((PLpgSQL_exception *) lfirst(l))->action,
														expr_walker,
															    context))
							return true;
					}
				}

				return false;
			}

		case PLPGSQL_STMT_ASSIGN:
			return expr_walker(stmt, ((PLpgSQL_stmt_assign *) stmt)->expr, context);

		case PLPGSQL_STMT_IF:
			{
				PLpgSQL_stmt_if *stmt_if = (PLpgSQL_stmt_if *) stmt;

#if PG_VERSION_NUM >= 90200
				ListCell *l;
#endif

				if (expr_walker(stmt, stmt_if->cond, context))
					return true;

#if PG_VERSION_NUM >= 90200

				if (plpgsql_lint_expr_walker_list(func, stmt_if->then_body, expr_walker, context))
						return true;


				foreach(l, stmt_if->elsif_list)
				{
					PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(l);

					if (expr_walker(stmt, elif->cond, context))
						return true;

					if (plpgsql_lint_expr_walker_list(func, elif->stmts, expr_walker, context))
						return true;

				}

				return plpgsql_lint_expr_walker_list(func, stmt_if->else_body, expr_walker, context);

#else
				if (plpgsql_lint_expr_walker_list(func, stmt_if->true_body, expr_walker, context))
					return true;

				return plpgsql_lint_expr_walker_list(func, stmt_if->false_body, expr_walker, context);

#endif

			}

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;

				if (expr_walker(stmt, stmt_case->t_expr, context))
					return true;

				foreach(l, stmt_case->case_when_list)
				{
					PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);

					if (expr_walker(stmt, cwt->expr, context))
						return true;

					if (plpgsql_lint_expr_walker_list(func, cwt->stmts, expr_walker, context))
						return true;
				}

				return plpgsql_lint_expr_walker_list(func, stmt_case->else_stmts, expr_walker, context);
			}

		case PLPGSQL_STMT_LOOP:
			return plpgsql_lint_expr_walker_list(func, ((PLpgSQL_stmt_loop *) stmt)->body, expr_walker, context);

		case PLPGSQL_STMT_WHILE:
			{
				PLpgSQL_stmt_while *stmt_while = (PLpgSQL_stmt_while *) stmt;

				if (expr_walker(stmt, stmt_while->cond, context))
					return true;

				return plpgsql_lint_expr_walker_list(func, stmt_while->body, expr_walker, context);
			}

		case PLPGSQL_STMT_FORI:
			{
				PLpgSQL_stmt_fori *stmt_fori = (PLpgSQL_stmt_fori *) stmt;

				if (expr_walker(stmt, stmt_fori->lower, context))
					return true;

				if (expr_walker(stmt, stmt_fori->upper, context))
					return true;

				if (expr_walker(stmt, stmt_fori->step, context))
					return true;

				return plpgsql_lint_expr_walker_list(func, stmt_fori->body, expr_walker, context);
			}

		case PLPGSQL_STMT_FORS:
			{
				PLpgSQL_stmt_fors *stmt_fors = (PLpgSQL_stmt_fors *) stmt;

				if (expr_walker(stmt, stmt_fors->query, context))
					return true;

				return plpgsql_lint_expr_walker_list(func, stmt_fors->body, expr_walker, context);
			}

		case PLPGSQL_STMT_FORC:
			{
				PLpgSQL_stmt_forc *stmt_forc = (PLpgSQL_stmt_forc *) stmt;
				PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_forc->curvar];

				if (expr_walker(stmt, stmt_forc->argquery, context))
					return true;

				if (expr_walker(stmt, var->cursor_explicit_expr, context))
					return true;

				return plpgsql_lint_expr_walker_list(func, stmt_forc->body, expr_walker, context);
			}

		case PLPGSQL_STMT_DYNFORS:
			{
				PLpgSQL_stmt_dynfors * stmt_dynfors = (PLpgSQL_stmt_dynfors *) stmt;

				if (expr_walker(stmt, stmt_dynfors->query, context))
					return true;

				foreach(l, stmt_dynfors->params)
				{
					if (expr_walker(stmt, (PLpgSQL_expr *) lfirst(l), context))
						return true;
				}

				return plpgsql_lint_expr_walker_list(func, stmt_dynfors->body, expr_walker, context);
			}

#if PG_VERSION_NUM >= 90100

		case PLPGSQL_STMT_FOREACH_A:
			{
				PLpgSQL_stmt_foreach_a *stmt_foreach_a = (PLpgSQL_stmt_foreach_a *) stmt;

				if (expr_walker(stmt, stmt_foreach_a->expr, context))
					return true;

				return plpgsql_lint_expr_walker_list(func, stmt_foreach_a->body, expr_walker, context);
			}

#endif

		case PLPGSQL_STMT_EXIT:
			return expr_walker(stmt, ((PLpgSQL_stmt_exit *) stmt)->cond, context);

		case PLPGSQL_STMT_PERFORM:
			return expr_walker(stmt, ((PLpgSQL_stmt_perform *) stmt)->expr, context);

		case PLPGSQL_STMT_RETURN:
			return expr_walker(stmt, ((PLpgSQL_stmt_return *) stmt)->expr, context);

		case PLPGSQL_STMT_RETURN_NEXT:
			return expr_walker(stmt, ((PLpgSQL_stmt_return_next *) stmt)->expr, context);

		case PLPGSQL_STMT_RETURN_QUERY:
			{
				PLpgSQL_stmt_return_query *stmt_rq = (PLpgSQL_stmt_return_query *) stmt;

				if (expr_walker(stmt, stmt_rq->query, context))
					return true;

				if (expr_walker(stmt, stmt_rq->dynquery, context))
					return true;

				foreach(l, stmt_rq->params)
				{
					if (expr_walker(stmt, (PLpgSQL_expr *) lfirst(l), context))
						return true;
				}

				return false;
			}

		case PLPGSQL_STMT_RAISE:
			{
				PLpgSQL_stmt_raise *stmt_raise = (PLpgSQL_stmt_raise *) stmt;

				foreach(l, stmt_raise->params)
				{
					if (expr_walker(stmt, (PLpgSQL_expr *) lfirst(l), context))
						return true;
				}
				foreach(l, stmt_raise->options)
				{
					if (expr_walker(stmt, ((PLpgSQL_raise_option *) lfirst(l))->expr, context))
						return true;
				}

				return false;
			}

		case PLPGSQL_STMT_EXECSQL:
			return expr_walker(stmt, ((PLpgSQL_stmt_execsql *) stmt)->sqlstmt, context);

		case PLPGSQL_STMT_DYNEXECUTE:
			{
				PLpgSQL_stmt_dynexecute *stmt_dynexecute = (PLpgSQL_stmt_dynexecute *) stmt;

				if (expr_walker(stmt, stmt_dynexecute->query, context))
					return true;

				foreach(l, stmt_dynexecute->params)
				{
					if (expr_walker(stmt, (PLpgSQL_expr *) lfirst(l), context))
						return true;
				}

				return false;
			}
			break;

		case PLPGSQL_STMT_GETDIAG:
			return false;

		case PLPGSQL_STMT_OPEN:
			{
				PLpgSQL_stmt_open *stmt_open = (PLpgSQL_stmt_open *) stmt;

				PLpgSQL_var *var = (PLpgSQL_var *) func->datums[stmt_open->curvar];

				if (expr_walker(stmt, var->cursor_explicit_expr, context))
					return true;

				if (expr_walker(stmt, stmt_open->query, context))
					return true;

				if (expr_walker(stmt, stmt_open->dynquery, context))
					return true;

				if (expr_walker(stmt, stmt_open->argquery, context))
					return true;

				foreach(l, stmt_open->params)
				{
					if (expr_walker(stmt, (PLpgSQL_expr *) lfirst(l), context))
						return true;
				}

				return false;
			}

		case PLPGSQL_STMT_FETCH:
			return expr_walker(stmt, NULL, context);

		case PLPGSQL_STMT_CLOSE:
			return false;

		default:
			elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
			return false; /* be compiler quite */
	}
}

/*
 * Sometime we must initialize a unknown record variable with NULL
 * of type that is derived from some plan. This is necessary for later
 * using a rec variable. Last parameter 'use_element_type' is true, when
 * we would to assign a element type of result array.
 *
 */
static void
assign_result_desc(PLpgSQL_execstate *estate,
				    PLpgSQL_stmt *stmt,
					    PLpgSQL_rec *rec,
						    PLpgSQL_expr *query,
							    bool use_element_type)
{
	bool	   *nulls;
	HeapTuple  tup;
	CachedPlanSource *plansource = NULL;
	const char *err_text = estate->err_text;

	estate->err_text = NULL;
	estate->err_stmt = stmt;

	if (rec->freetup)
		heap_freetuple(rec->tup);

	if (rec->freetupdesc)
		FreeTupleDesc(rec->tupdesc);

	if (query->plan != NULL)
	{
		SPIPlanPtr plan = query->plan;

		if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
			elog(ERROR, "cached plan is not valid plan");

		if (list_length(plan->plancache_list) != 1)
			elog(ERROR, "plan is not single execution plan");

		plansource = (CachedPlanSource *) linitial(plan->plancache_list);

		rec->tupdesc = CreateTupleDescCopy(plansource->resultDesc);
		rec->freetupdesc = true;
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
		TupleDesc tupdesc;

		/* result should be a array */
		if (rec->tupdesc->natts != 1)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg_plural("query \"%s\" returned %d column",
							   "query \"%s\" returned %d columns",
							   rec->tupdesc->natts,
							   query->query,
							   rec->tupdesc->natts)));

		/* check the type of the expression - must be an array */
		elemtype = get_element_type(rec->tupdesc->attrs[0]->atttypid);
		if (!OidIsValid(elemtype))
			ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("FOREACH expression must yield an array, not type %s",
						format_type_be(rec->tupdesc->attrs[0]->atttypid))));

		/* we can't know typmod now */
		tupdesc = lookup_rowtype_tupdesc_noerror(elemtype, -1, true);
		if (tupdesc != NULL)
		{
			if (rec->freetupdesc)
				FreeTupleDesc(rec->tupdesc);
			rec->tupdesc = CreateTupleDescCopy(tupdesc);
			rec->freetupdesc = true;
			ReleaseTupleDesc(tupdesc);
		}
		else
			elog(ERROR, "cannot to identify real type for record type variable");
	}

	/*
	 * One spacial case is when record is assigned to composite type, then 
	 * we should to unpack composite type.
	 */
	if (rec->tupdesc->tdtypeid == RECORDOID &&
			rec->tupdesc->tdtypmod == -1 &&
			rec->tupdesc->natts == 1 &&
			stmt->cmd_type == PLPGSQL_STMT_ASSIGN)
	{
		TupleDesc tupdesc;

		tupdesc = lookup_rowtype_tupdesc_noerror(rec->tupdesc->attrs[0]->atttypid,
								rec->tupdesc->attrs[0]->atttypmod,
												    true);
		if (tupdesc != NULL)
		{
			if (rec->freetupdesc)
				FreeTupleDesc(rec->tupdesc);
			rec->tupdesc = CreateTupleDescCopy(tupdesc);
			rec->freetupdesc = true;
			ReleaseTupleDesc(tupdesc);
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
	if (rec->tupdesc->tdtypeid == RECORDOID &&
			rec->tupdesc->tdtypmod == -1 &&
			rec->tupdesc->natts == 1 &&
			rec->tupdesc->attrs[0]->atttypid == RECORDOID &&
			rec->tupdesc->attrs[0]->atttypmod == -1)
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

					FreeTupleDesc(rec->tupdesc);
					BlessTupleDesc(rd);

					rec->tupdesc = rd;
				}
			}
		}

#if PG_VERSION_NUM >= 90200

		ReleaseCachedPlan(cplan, true);

#endif
	}

	/* last recheck */
	if (rec->tupdesc->tdtypeid == RECORDOID &&
			rec->tupdesc->tdtypmod == -1 &&
			rec->tupdesc->natts == 1 &&
			rec->tupdesc->attrs[0]->atttypid == RECORDOID &&
			rec->tupdesc->attrs[0]->atttypmod == -1)
		elog(ERROR, "cannot to identify real type for record type variable");

	/* initialize rec by NULLs */
	nulls = (bool *) palloc(rec->tupdesc->natts * sizeof(bool));
	memset(nulls, true, rec->tupdesc->natts * sizeof(bool));

	tup = heap_form_tuple(rec->tupdesc, NULL, nulls);
	if (HeapTupleIsValid(tup))
	{
		rec->tup = tup;
		rec->freetup = true;
	}
	else
	{
		rec->tup = NULL;
		rec->freetup = false;
	}

	estate->err_text = err_text;
}

/*
 * Prepare plans walker - this can be used for checking
 *
 */
static bool
plpgsql_lint_expr_prepare_plan(PLpgSQL_stmt *stmt, PLpgSQL_expr *expr, void *context)
{
	PLpgSQL_execstate *estate = (PLpgSQL_execstate *) context;
	int cursorOptions = 0;
	const char *err_text = estate->err_text;

	if (stmt == NULL)
		return false;

	/* overwrite a estate variables */
	estate->err_text = NULL;
	estate->err_stmt = stmt;

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

	/*
	 * If first time through, create a plan for this expression.
	 */
	if (expr != NULL && expr->plan == NULL)
	{
		exec_prepare_plan(estate, expr, cursorOptions);
	}

	/*
	 * very common practic in PLpgSQL is  is using a record type. But any using of
	 * untyped record breaks a check. A solution is an prediction of record type based
	 * on plans - a following switch covers all PLpgSQL statements where a record
	 * variable can be assigned.
	 *
	 * when record is target of dynamic SQL statement, then raise exception
	 *
	 */
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_ASSIGN:
			{
				PLpgSQL_stmt_assign *stmt_assign = (PLpgSQL_stmt_assign *) stmt;
				PLpgSQL_datum *target = (estate->datums[stmt_assign->varno]);

				if (target->dtype == PLPGSQL_DTYPE_REC)
				{
					assign_result_desc(estate, stmt,
								(PLpgSQL_rec *) target,
									    stmt_assign->expr,
												false);
				}
			}
			break;

		case PLPGSQL_STMT_EXECSQL:
			{
				PLpgSQL_stmt_execsql *stmt_execsql = (PLpgSQL_stmt_execsql *) stmt;

				if (stmt_execsql->rec != NULL)
				{
					assign_result_desc(estate, stmt,
								(PLpgSQL_rec *) (estate->datums[stmt_execsql->rec->dno]),
									stmt_execsql->sqlstmt,
												false);
				}
			}
			break;

		case PLPGSQL_STMT_FETCH:
			{
				PLpgSQL_stmt_fetch *stmt_fetch = (PLpgSQL_stmt_fetch *) stmt;

				/* fetch can not determinate a record datatype for refcursors */
				if (stmt_fetch->rec != NULL)
				{
					PLpgSQL_var *curvar = (PLpgSQL_var *)( estate->datums[stmt_fetch->curvar]);
					PLpgSQL_rec *rec = (PLpgSQL_rec *) (estate->datums[stmt_fetch->rec->dno]);

					if (curvar->cursor_explicit_expr == NULL)
						elog(ERROR, "cannot to determinate record type for refcursor");

					assign_result_desc(estate, stmt,
								rec,
									curvar->cursor_explicit_expr,
											    false);
				}
			}
			break;

		case PLPGSQL_STMT_FORS:
			{
				PLpgSQL_stmt_fors *stmt_fors = (PLpgSQL_stmt_fors *) stmt;

				if (stmt_fors->rec != NULL)
				{
					assign_result_desc(estate, stmt,
								(PLpgSQL_rec *) (estate->datums[stmt_fors->rec->dno]),
									stmt_fors->query,
											    false);
				}
			}
			break;

		case PLPGSQL_STMT_FORC:
			{
				PLpgSQL_stmt_forc *stmt_forc = (PLpgSQL_stmt_forc *) stmt;
				PLpgSQL_var *curvar = (PLpgSQL_var *) (estate->datums[stmt_forc->curvar]);

				if (stmt_forc->rec != NULL && curvar->cursor_explicit_expr == expr)
				{
					PLpgSQL_rec *rec = (PLpgSQL_rec *) (estate->datums[stmt_forc->rec->dno]);

					assign_result_desc(estate, stmt,
									rec,
									curvar->cursor_explicit_expr,
											    false);
				}
			}
			break;

#if PG_VERSION_NUM >= 90100

		case PLPGSQL_STMT_FOREACH_A:
			{
				PLpgSQL_stmt_foreach_a *stmt_foreach_a = (PLpgSQL_stmt_foreach_a *) stmt;
				PLpgSQL_datum *loop_var = estate->datums[stmt_foreach_a->varno];

				if (loop_var->dtype == PLPGSQL_DTYPE_REC)
				{
					assign_result_desc(estate, stmt,
								(PLpgSQL_rec *) loop_var,
									stmt_foreach_a->expr,
											    true);
				}
			}
			break;

#endif

		case PLPGSQL_STMT_CASE:
			{
				PLpgSQL_stmt_case *stmt_case = (PLpgSQL_stmt_case *) stmt;
				TupleDesc tupdesc;
				Oid result_oid;

				/*
				 * this is special case - a result type of expression should to
				 * overwrite a expected int datatype.
				 */
				if (stmt_case->t_expr == expr)
				{
					CachedPlanSource *plansource = NULL;
					const char *err_text = estate->err_text;

					estate->err_text = NULL;
					estate->err_stmt = stmt;


					if (expr  != NULL && expr->plan != NULL)
					{
						SPIPlanPtr plan = expr->plan;
						PLpgSQL_var *t_var = (PLpgSQL_var *) estate->datums[stmt_case->t_varno];

						if (plan == NULL || plan->magic != _SPI_PLAN_MAGIC)
							elog(ERROR, "cached plan is not valid plan");

						if (list_length(plan->plancache_list) != 1)
							elog(ERROR, "plan is not single execution plan");

						plansource = (CachedPlanSource *) linitial(plan->plancache_list);
						tupdesc = CreateTupleDescCopy(plansource->resultDesc);

						if (tupdesc->natts != 1)
							ereport(ERROR,
								    (errcode(ERRCODE_SYNTAX_ERROR),
								     errmsg_plural("query \"%s\" returned %d column",
									   "query \"%s\" returned %d columns",
										    tupdesc->natts,
										    expr->query,
										    tupdesc->natts)));

						result_oid = tupdesc->attrs[0]->atttypid;

						/*
						 * When expected datatype is different from real, change it. Note that
						 * what we're modifying here is an execution copy of the datum, so
						 * this doesn't affect the originally stored function parse tree.
						 */

#if PG_VERSION_NUM >= 90100

						if (t_var->datatype->typoid != result_oid)
							t_var->datatype = plpgsql_build_datatype(result_oid,
															 -1,
														   estate->func->fn_input_collation);
#else

							t_var->datatype = plpgsql_build_datatype(result_oid, -1);

#endif

						FreeTupleDesc(tupdesc);
					}
					else
						elog(ERROR, "there are no plan for query: \"%s\"",
											expr->query);

					estate->err_text = err_text;
				}
			}
			break;

		case PLPGSQL_STMT_DYNEXECUTE:
			{
				PLpgSQL_stmt_dynexecute *stmt_dynexecute = (PLpgSQL_stmt_dynexecute *) stmt;

				if (stmt_dynexecute->into && stmt_dynexecute->rec != NULL)
					elog(ERROR, "cannot to determine a result of dynamic SQL");
			}
			break;

		case PLPGSQL_STMT_DYNFORS:
			{
				PLpgSQL_stmt_dynfors *stmt_dynfors = (PLpgSQL_stmt_dynfors *) stmt;

				if (stmt_dynfors->rec != NULL)
					elog(ERROR, "cannot to determinate a result of dynamic SQL");
			}
			break;
	}



	estate->err_text = err_text;

	if (expr != NULL)
	{
		SPI_freeplan(expr->plan);
		expr->plan = NULL;
	}

	return false;
}
